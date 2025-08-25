"""
Módulo VendasService
====================

Serviço responsável por registrar **vendas** no sistema, aplicar a
**liquidação** (caixa/banco) na data correta e gravar **log idempotente**
em `movimentacoes_bancarias`.
"""

from __future__ import annotations

from typing import Optional, Tuple, Any
import re
import sqlite3
from datetime import datetime

import pandas as pd

from shared.db import get_conn
from shared.ids import uid_venda_liquidacao, sanitize

__all__ = ["VendasService"]


# -----------------------------------------------------------------------------#
# Helper de taxa (consulta a tabela de taxas da maquineta)
# -----------------------------------------------------------------------------#
def _resolver_taxa_percentual(
    conn: sqlite3.Connection,
    *,
    forma: str,
    bandeira: Optional[str],
    parcelas: int,
    maquineta: Optional[str],
) -> float:
    """
    Busca na tabela `taxas_maquinas` uma taxa compatível com
    (forma, bandeira, parcelas, maquineta). Retorna 0.0 se não encontrar.
    """
    try:
        row = pd.read_sql(
            """
            SELECT COALESCE(taxa_percentual,0) AS taxa
              FROM taxas_maquinas
             WHERE UPPER(forma)=?
               AND (bandeira IS NULL OR bandeira=?)
               AND (parcelas IS NULL OR parcelas=?)
               AND (maquineta IS NULL OR maquineta=?)
             ORDER BY 
               CASE WHEN bandeira IS NULL THEN 1 ELSE 0 END,
               CASE WHEN parcelas IS NULL THEN 1 ELSE 0 END,
               CASE WHEN maquineta IS NULL THEN 1 ELSE 0 END
             LIMIT 1
            """,
            conn,
            params=[(forma or "").upper(), bandeira, int(parcelas or 1), maquineta],
        )
        if not row.empty:
            return float(row.loc[0, "taxa"]) or 0.0
    except Exception:
        pass
    return 0.0


class VendasService:
    """Regras de negócio para registro de vendas."""

    # ------------------------------------------------------------------ #
    # Setup
    # ------------------------------------------------------------------ #
    def __init__(self, db_path_like: Any) -> None:
        """
        Inicializa o serviço.

        Args:
            db_path_like (Any): Caminho do SQLite (str/Path) ou objeto com
                atributo de caminho (ex.: SimpleNamespace(caminho_banco=...)).
        """
        self.db_path_like = db_path_like  # get_conn aceita db_path_like direto.

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
    # Insert em `entrada`
    # =============================
    def _insert_entrada(
        self,
        conn: sqlite3.Connection,
        *,
        data_venda: str,
        data_liq: str,
        valor_bruto: float,
        valor_liquido: float | None,
        forma: str,
        parcelas: int,
        bandeira: Optional[str],
        maquineta: Optional[str],           # None para PIX direto / dinheiro
        banco_destino: Optional[str],
        taxa_percentual: Optional[float],   # se None -> buscar tabela
        usuario: str,
    ) -> int:
        """
        Insere a venda na tabela `entrada`.

        Regras:
        - `Valor` sempre recebe o valor bruto.
        - `valor_liquido` = bruto se não houver taxa; senão líquido com desconto.
        - `Forma_de_Pagamento` sempre preenchida (DINHEIRO, PIX, etc).
        - DINHEIRO / PIX direto -> taxa=0, maquineta=NULL.
        - PIX via maquineta / DÉBITO / CRÉDITO -> aplica taxa da tabela.
        - Garante colunas: Usuario, valor_liquido, maquineta, created_at.
        """
        cols_df = pd.read_sql("PRAGMA table_info(entrada);", conn)
        colnames = set(cols_df["name"].astype(str).tolist())

        # garantir colunas obrigatórias
        if "Usuario" not in colnames:
            conn.execute('ALTER TABLE entrada ADD COLUMN "Usuario" TEXT;'); colnames.add("Usuario")
        if "valor_liquido" not in colnames:
            conn.execute('ALTER TABLE entrada ADD COLUMN "valor_liquido" REAL;'); colnames.add("valor_liquido")
        if "maquineta" not in colnames:
            conn.execute('ALTER TABLE entrada ADD COLUMN "maquineta" TEXT;'); colnames.add("maquineta")
        if "created_at" not in colnames:
            conn.execute('ALTER TABLE entrada ADD COLUMN "created_at" TEXT DEFAULT (CURRENT_TIMESTAMP);'); colnames.add("created_at")

        forma_upper = (forma or "").upper()
        parcelas = int(parcelas or 1)

        # decidir taxa efetiva
        if forma_upper == "DINHEIRO":
            taxa_eff, maq_eff = 0.0, None
        elif forma_upper == "PIX" and not (maquineta and maquineta.strip()):
            taxa_eff, maq_eff = 0.0, None  # PIX direto
        else:
            if taxa_percentual is None:
                taxa_eff = _resolver_taxa_percentual(
                    conn, forma=forma_upper, bandeira=bandeira,
                    parcelas=parcelas, maquineta=maquineta
                )
            else:
                taxa_eff = float(taxa_percentual)
            maq_eff = maquineta

        # calcular líquido
        if valor_liquido is None:
            liquido = float(valor_bruto) if taxa_eff == 0.0 else round(float(valor_bruto) * (1 - taxa_eff / 100.0), 2)
        else:
            liquido = float(valor_liquido)

        # montar INSERT (usar None para gravar NULL em campos opcionais)
        to_insert = {
            "Data": data_venda,
            "Data_Liq": data_liq,
            "Valor": float(valor_bruto),
            "valor_liquido": liquido,
            "Forma_de_Pagamento": forma_upper,
            "Parcelas": parcelas,
            "Bandeira": bandeira or None,
            "maquineta": maq_eff,
            "Banco_Destino": banco_destino or None,
            "Usuario": usuario,
            "created_at": datetime.now().isoformat(timespec="seconds"),
        }

        if "Taxa_percentual" in colnames:
            to_insert["Taxa_percentual"] = float(taxa_eff)
        elif "Taxa_Percentual" in colnames:
            to_insert["Taxa_Percentual"] = float(taxa_eff)

        names, values = [], []
        for k, v in to_insert.items():
            if k in colnames and v is not None:
                names.append(f'"{k}"'); values.append(v)

        placeholders = ", ".join("?" for _ in names)
        cols_sql = ", ".join(names)
        conn.execute(f"INSERT INTO entrada ({cols_sql}) VALUES ({placeholders})", values)
        return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    # =============================
    # Regra principal (compat wrapper)
    # =============================
    def registrar_venda(self, *args, **kwargs) -> Tuple[int, int]:
        """
        Wrapper de compatibilidade:
        - Aceita 'caminho_banco' e usa como db_path_like (legado).
        - Mapeia nomes alternativos de parâmetros:
          data|data_venda, data_liq|data_liquidacao,
          valor|valor_bruto, forma|forma_pagamento,
          taxa|taxa_percentual.
        - Encaminha para _registrar_venda_impl.
        """
        if "caminho_banco" in kwargs and kwargs["caminho_banco"]:
            self.db_path_like = kwargs.pop("caminho_banco")

        data_venda = kwargs.pop("data_venda", kwargs.pop("data", None))
        data_liq = kwargs.pop("data_liq", kwargs.pop("data_liquidacao", None))
        valor_bruto = kwargs.pop("valor_bruto", kwargs.pop("valor", None))
        forma = kwargs.pop("forma", kwargs.pop("forma_pagamento", None))
        parcelas = kwargs.pop("parcelas", 1)
        bandeira = kwargs.pop("bandeira", None)
        maquineta = kwargs.pop("maquineta", None)
        banco_destino = kwargs.pop("banco_destino", None)
        taxa_percentual = kwargs.pop("taxa_percentual", kwargs.pop("taxa", 0.0))
        usuario = kwargs.pop("usuario", "Sistema")

        return self._registrar_venda_impl(
            data_venda=data_venda,
            data_liq=data_liq,
            valor_bruto=valor_bruto,
            forma=forma,
            parcelas=parcelas,
            bandeira=bandeira,
            maquineta=maquineta,
            banco_destino=banco_destino,
            taxa_percentual=taxa_percentual,
            usuario=usuario,
        )

    # =============================
    # Regra principal (implementação real)
    # =============================
    def _registrar_venda_impl(
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

        with get_conn(self.db_path_like) as conn:
            # decidir taxa efetiva com base no fluxo
            if forma_u == "DINHEIRO" or (forma_u == "PIX" and not (maquineta and maquineta.strip())):
                taxa_eff = 0.0
            else:
                taxa_eff = float(taxa_percentual or 0.0)
                if taxa_eff == 0.0:
                    taxa_eff = _resolver_taxa_percentual(
                        conn,
                        forma=forma_u,
                        bandeira=bandeira,
                        parcelas=int(parcelas),
                        maquineta=maquineta,
                    )

            valor_liquido = round(float(valor_bruto) * (1.0 - float(taxa_eff) / 100.0), 2)

            # Idempotência — um único log por liquidação
            trans_uid = uid_venda_liquidacao(
                data_venda,
                data_liq,
                float(valor_bruto),
                forma_u,
                int(parcelas),
                bandeira,
                maquineta,
                banco_destino,
                float(taxa_eff),
                usuario,
            )

            # Se já existe movimentação com esse trans_uid, não duplica
            row = conn.execute(
                "SELECT id FROM movimentacoes_bancarias WHERE trans_uid=? LIMIT 1;",
                (trans_uid,),
            ).fetchone()
            if row:
                return (-1, -1)

            cur = conn.cursor()

            # 1) INSERT em `entrada`
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
                taxa_percentual=float(taxa_eff),
                usuario=usuario,
            )

            # 2) Atualiza saldos na data de liquidação
            if forma_u == "DINHEIRO":
                self._garantir_linha_saldos_caixas(conn, data_liq)
                cur.execute(
                    "UPDATE saldos_caixas SET caixa_vendas = COALESCE(caixa_vendas,0) + ? WHERE data = ?",
                    (float(valor_liquido), data_liq),
                )
                banco_label = "Caixa_Vendas"
            else:
                if not banco_destino:
                    raise ValueError("banco_destino é obrigatório para formas não-DINHEIRO.")
                self._garantir_linha_saldos_bancos(conn, data_liq)
                self._ajustar_banco_dynamic(
                    conn,
                    banco_col=banco_destino,
                    delta=float(valor_liquido),
                    data=data_liq,
                )
                banco_label = banco_destino

            # 3) Log idempotente em movimentacoes_bancarias
            obs = (
                f"Liquidação venda {forma_u} {parcelas}x - "
                f"{bandeira or ''}/{maquineta or ''} • Bruto R$ {valor_bruto:.2f} • "
                f"Taxa {taxa_eff:.2f}% -> Líquido R$ {valor_liquido:.2f}"
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
