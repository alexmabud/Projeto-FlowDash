"""
Módulo VendasService
====================

Serviço responsável por registrar **vendas** no sistema, aplicar a
**liquidação** (caixa/banco) na data correta e gravar **log idempotente**
em `movimentacoes_bancarias`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple, Dict, Any
import re
import sqlite3
import pandas as pd

from shared.db import get_conn
from shared.ids import uid_venda_liquidacao, sanitize
from repository.movimentacoes_repository import MovimentacoesRepository


__all__ = ["VendasService"]


class VendasService:
    """Regras de negócio para registro de vendas."""

    # ------------------------------------------------------------------ #
    # Setup
    # ------------------------------------------------------------------ #
    def __init__(self, db_path: str) -> None:
        """Inicializa o serviço e garante schema de movimentações.

        Args:
            db_path (str): Caminho para o arquivo de banco de dados SQLite.
        """
        self.db_path = db_path
        self.mov_repo = MovimentacoesRepository(db_path)

    # =============================
    # Infraestrutura interna
    # =============================
    def _garantir_linha_saldos_caixas(self, conn: sqlite3.Connection, data: str) -> None:
        """Garante existência da linha em `saldos_caixas` para a data."""
        cur = conn.execute("SELECT 1 FROM saldos_caixas WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute(
                """
                INSERT INTO saldos_caixas
                    (data, caixa, caixa_2, caixa_vendas, caixa2_dia, caixa_total, caixa2_total)
                VALUES (?, 0, 0, 0, 0, 0, 0)
                """,
                (data,),
            )

    def _garantir_linha_saldos_bancos(self, conn: sqlite3.Connection, data: str) -> None:
        """Garante existência da linha em `saldos_bancos` para a data."""
        cur = conn.execute("SELECT 1 FROM saldos_bancos WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("INSERT OR IGNORE INTO saldos_bancos (data) VALUES (?)", (data,))

    _COL_RE = re.compile(r"^[A-Za-z0-9_ ]{1,64}$")

    def _validar_nome_coluna_banco(self, banco_col: str) -> str:
        banco_col = (banco_col or "").strip()
        if not self._COL_RE.match(banco_col):
            raise ValueError(f"Nome de banco/coluna inválido: {banco_col!r}")
        return banco_col

    def _ajustar_banco_dynamic(
        self, conn: sqlite3.Connection, banco_col: str, delta: float, data: str
    ) -> None:
        """Ajusta dinamicamente a coluna do banco em `saldos_bancos`."""
        banco_col = self._validar_nome_coluna_banco(banco_col)
        cols = pd.read_sql("PRAGMA table_info(saldos_bancos);", conn)["name"].tolist()
        if banco_col not in cols:
            conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_col}" REAL DEFAULT 0.0;')
        self._garantir_linha_saldos_bancos(conn, data)
        conn.execute(
            f'UPDATE saldos_bancos SET "{banco_col}" = COALESCE("{banco_col}", 0) + ? WHERE data = ?',
            (float(delta), data),
        )

    # =============================
    # Helpers de schema/insert
    # =============================
    def _columns(self, conn: sqlite3.Connection, table: str) -> Dict[str, Dict[str, Any]]:
        """Retorna metadados de colunas (name -> info) para uma tabela."""
        rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
        return {str(r[1]): {"cid": r[0], "name": r[1], "type": r[2], "notnull": r[3], "dflt": r[4]} for r in rows}

    def _insert_entrada(
        self,
        conn: sqlite3.Connection,
        *,
        data_venda: str,
        data_liq: str,
        valor_bruto: float,
        valor_liquido: float,
        forma: str,
        parcelas: int,
        bandeira: Optional[str],
        maquineta: Optional[str],
        banco_destino: Optional[str],
        taxa_percentual: float,
        usuario: str,
    ) -> int:
        """
        Insere a venda na tabela `entrada`, adaptando-se ao schema existente.
        Tenta usar colunas canônicas; se alguma não existir, ignora-a no INSERT.
        """
        cols = self._columns(conn, "entrada")
        # mapa de possíveis nomes → valor
        candidatos = [
            ("Data", data_venda),
            ("Data_Liq", data_liq),
            ("Data_Liquidacao", data_liq),
            ("Valor_Bruto", float(valor_bruto)),
            ("Valor_Liquido", float(valor_liquido)),
            ("Valor", float(valor_liquido) if "Valor" in cols else None),
            ("Forma_de_Pagamento", forma),
            ("Forma", forma),
            ("Parcelas", int(parcelas)),
            ("Bandeira", bandeira or ""),
            ("Maquineta", maquineta or ""),
            ("Banco_Destino", banco_destino or ""),
            ("Taxa_Percentual", float(taxa_percentual)),
            ("Usuario", usuario),
            ("Usuario_Cadastro", usuario),
            # colunas comuns para UI
            ("Observacao", f"Venda {forma} {parcelas}x - {bandeira or ''}/{maquineta or ''}".strip(" -/")),
        ]
        names = []
        values = []
        for nome, valor in candidatos:
            if valor is None:
                continue
            if nome in cols:
                names.append(nome)
                values.append(valor)

        if not names:
            # Mínimo viável: se não sabemos o schema, gravar ao menos Valor/Data
            names = ["Data", "Valor"]
            values = [data_venda, float(valor_liquido)]

        placeholders = ", ".join("?" for _ in names)
        colnames = ", ".join(names)
        conn.execute(f"INSERT INTO entrada ({colnames}) VALUES ({placeholders})", values)
        return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    # =============================
    # Regra principal
    # =============================
    def registrar_venda(
        self,
        data_venda: str,            # YYYY-MM-DD
        data_liq: str,              # YYYY-MM-DD
        valor_bruto: float,
        forma: str,                 # "DINHEIRO" | "PIX" | "DÉBITO" | "CRÉDITO" | "LINK_PAGAMENTO"
        parcelas: int,
        bandeira: Optional[str],
        maquineta: Optional[str],
        banco_destino: Optional[str],
        taxa_percentual: float,
        usuario: str,
    ) -> Tuple[int, int]:
        """Registra a venda, aplica a liquidação e grava log idempotente.

        Fluxo:
          1. Insere em `entrada` (valor bruto e líquido).
          2. Atualiza saldos na `data_liq` (caixa_vendas **ou** banco).
          3. Registra **um** log de liquidação em `movimentacoes_bancarias`
             protegido por idempotência (via `trans_uid`).

        Returns:
            (venda_id, mov_id) ou (-1, -1) se idempotente.
        """
        # Validações básicas
        try:
            pd.to_datetime(data_venda)
            pd.to_datetime(data_liq)
        except Exception:
            raise ValueError("Datas inválidas; use YYYY-MM-DD.")
        if float(valor_bruto) <= 0:
            raise ValueError("valor_bruto deve ser > 0.")
        forma_u = sanitize(forma or "").upper()
        if forma_u == "DEBITO":
            forma_u = "DÉBITO"
        if forma_u not in ("DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"):
            raise ValueError(f"Forma de pagamento inválida: {forma!r}")
        parcelas = int(parcelas or 1)
        if parcelas < 1:
            raise ValueError("parcelas deve ser >= 1.")

        bandeira = sanitize(bandeira)
        maquineta = sanitize(maquineta)
        banco_destino = sanitize(banco_destino)
        usuario = sanitize(usuario)

        # Cálculo do líquido (desconta taxa da adquirente/PSP)
        taxa_percentual = float(taxa_percentual or 0.0)
        valor_liquido = round(float(valor_bruto) * (1.0 - taxa_percentual / 100.0), 2)

        # Idempotência — um único log por liquidação
        trans_uid = uid_venda_liquidacao(
            data_venda, data_liq, float(valor_bruto), forma_u, int(parcelas),
            bandeira, maquineta, banco_destino, float(taxa_percentual), usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            # Já liquidado/logado
            return (-1, -1)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # 1) INSERT em `entrada` (adaptativo ao schema)
            venda_id = self._insert_entrada(
                conn,
                data_venda=data_venda,
                data_liq=data_liq,
                valor_bruto=float(valor_bruto),
                valor_liquido=float(valor_liquido),
                forma=forma_u,
                parcelas=int(parcelas),
                bandeira=bandeira,
                maquineta=maquineta,
                banco_destino=banco_destino,
                taxa_percentual=float(taxa_percentual),
                usuario=usuario,
            )

            # 2) Atualiza saldos na data de liquidação
            if forma_u == "DINHEIRO":
                self._garantir_linha_saldos_caixas(conn, data_liq)
                # Atualiza caixa_vendas
                cur.execute(
                    "UPDATE saldos_caixas SET caixa_vendas = COALESCE(caixa_vendas,0) + ? WHERE data = ?",
                    (float(valor_liquido), data_liq),
                )
                banco_label = "Caixa_Vendas"
            else:
                if not banco_destino:
                    raise ValueError("banco_destino é obrigatório para formas não-DINHEIRO.")
                self._garantir_linha_saldos_bancos(conn, data_liq)
                self._ajustar_banco_dynamic(conn, banco_col=banco_destino, delta=float(valor_liquido), data=data_liq)
                banco_label = banco_destino

            # 3) Log idempotente em movimentacoes_bancarias
            obs = (
                f"Liquidação venda {forma_u} {parcelas}x - "
                f"{bandeira or ''}/{maquineta or ''} • Bruto R$ {valor_bruto:.2f} • "
                f"Taxa {taxa_percentual:.2f}% -> Líquido R$ {valor_liquido:.2f}"
            ).strip()

            cur.execute(
                """
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'entrada', ?, 'vendas_liquidacao', ?, 'entrada', ?, ?)
                """,
                (data_liq, banco_label, float(valor_liquido), obs, int(venda_id), trans_uid),
            )
            mov_id = int(cur.lastrowid)

            conn.commit()

        return (int(venda_id), int(mov_id))
