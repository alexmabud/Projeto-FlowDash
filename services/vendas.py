"""
Serviço de Vendas: entrada, liquidação, saldos e log idempotente.

Resumo
------
Implementa as regras de negócio para registrar vendas:
- insere a entrada (bruto e líquido),
- credita a liquidação no caixa/banco na data correta,
- registra log idempotente em `movimentacoes_bancarias`.

A camada de UI é responsável por calcular `data_liq` e fornecer
forma/maquineta/bandeira/parcelas/taxa/banco_destino.

Estilo
------
Docstrings padronizadas no estilo Google (pt-BR).
"""

# =============================
# Imports
# =============================

import pandas as pd
import sqlite3
from typing import Optional, Tuple
from datetime import datetime

from shared.db import get_conn
from shared.ids import uid_venda_liquidacao, sanitize
from repository.movimentacoes_repository import MovimentacoesRepository


# =============================
# Serviço de Vendas
# =============================

class VendasService:
    """Regras de negócio para registro de vendas.

    Centraliza a operação de registrar a venda (entrada), aplicar a liquidação
    em caixa/banco na data apropriada e gravar um log idempotente.
    """

    def __init__(self, db_path: str):
        """Inicializa o serviço com o caminho do banco de dados.

        Args:
            db_path (str): Caminho para o arquivo SQLite.
        """
        self.db_path = db_path
        self.mov_repo = MovimentacoesRepository(db_path)  # garante schema

    # =============================
    # Infraestrutura interna
    # =============================

    def _garantir_linha_saldos_caixas(self, conn, data: str):
        """Garante a existência da linha em `saldos_caixas` para a data.

        Se não existir, insere uma linha zerada para a data informada.

        Args:
            conn (sqlite3.Connection): Conexão SQLite aberta.
            data (str): Data no formato `YYYY-MM-DD`.
        """
        cur = conn.execute("SELECT 1 FROM saldos_caixas WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("""
                INSERT INTO saldos_caixas (data, caixa, caixa_2, caixa_vendas, caixa2_dia, caixa_total, caixa2_total)
                VALUES (?, 0, 0, 0, 0, 0, 0)
            """, (data,))

    def _garantir_linha_saldos_bancos(self, conn, data: str):
        """Garante a existência da linha em `saldos_bancos` para a data.

        Args:
            conn (sqlite3.Connection): Conexão SQLite aberta.
            data (str): Data no formato `YYYY-MM-DD`.
        """
        cur = conn.execute("SELECT 1 FROM saldos_bancos WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("INSERT OR IGNORE INTO saldos_bancos (data) VALUES (?)", (data,))

    def _ajustar_banco_dynamic(self, conn, banco_col: str, delta: float, data: str):
        """Ajusta dinamicamente a coluna do banco em `saldos_bancos`.

        Cria a coluna do banco (se necessário) e aplica o delta na data indicada.

        Args:
            conn (sqlite3.Connection): Conexão SQLite aberta.
            banco_col (str): Nome exato da coluna do banco.
            delta (float): Variação a aplicar (pode ser positiva ou negativa).
            data (str): Data do ajuste no formato `YYYY-MM-DD`.
        """
        # garante coluna dinâmica em saldos_bancos
        cols = pd.read_sql("PRAGMA table_info(saldos_bancos);", conn)["name"].tolist()
        if banco_col not in cols:
            conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_col}" REAL DEFAULT 0.0;')
        # aplica ajuste
        self._garantir_linha_saldos_bancos(conn, data)
        conn.execute(
            f'UPDATE saldos_bancos SET "{banco_col}" = COALESCE("{banco_col}",0) + ? WHERE data = ?',
            (float(delta), data)
        )

    # =============================
    # Regra principal
    # =============================

    def registrar_venda(
        self,
        data_venda: str,            # YYYY-MM-DD
        data_liq: str,              # YYYY-MM-DD
        valor_bruto: float,
        forma: str,                 # "DINHEIRO","PIX","DÉBITO","CRÉDITO","LINK_PAGAMENTO"
        parcelas: int,
        bandeira: Optional[str],
        maquineta: Optional[str],
        banco_destino: Optional[str],   # p/ DINHEIRO pode vir None
        taxa_percentual: float,
        usuario: str
    ) -> Tuple[int, int]:
        """Registra a venda, aplica a liquidação e grava log idempotente.

        Fluxo:
          1. INSERT em `entrada` (bruto + líquido);
          2. Atualiza saldos na `data_liq` (caixa_vendas ou banco);
          3. Registra um único log de liquidação em `movimentacoes_bancarias`
             protegido por idempotência (`trans_uid`).

        Args:
            data_venda (str): Data da venda (`YYYY-MM-DD`).
            data_liq (str): Data da liquidação (`YYYY-MM-DD`).
            valor_bruto (float): Valor bruto da venda.
            forma (str): Forma de pagamento.
            parcelas (int): Número de parcelas (1 para à vista).
            bandeira (Optional[str]): Bandeira do cartão (quando aplicável).
            maquineta (Optional[str]): Maquineta/PSP (quando aplicável).
            banco_destino (Optional[str]): Banco de destino (não usado para DINHEIRO).
            taxa_percentual (float): Taxa cobrada pela adquirente/PSP (%).
            usuario (str): Usuário responsável.

        Returns:
            Tuple[int, int]: `(venda_id, mov_id)`; `(-1, -1)` se bloqueado por idempotência.

        Raises:
            ValueError: Se valores obrigatórios estiverem inválidos.
        """
        # validações
        if float(valor_bruto) <= 0:
            raise ValueError("Valor da venda deve ser maior que zero.")
        forma = sanitize(forma).upper()
        if forma not in ("DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"):
            raise ValueError("Forma de pagamento inválida.")

        parcelas = int(parcelas or 1)
        bandeira = sanitize(bandeira)
        maquineta = sanitize(maquineta)
        usuario = sanitize(usuario)
        banco_destino = sanitize(banco_destino) if banco_destino else ""

        # valor líquido
        valor_liq = round(float(valor_bruto) * (1 - float(taxa_percentual or 0.0)/100.0), 2)

        # idempotência (1 único movimento: LIQUIDAÇÃO)
        banco_uid = "Caixa" if forma == "DINHEIRO" else banco_destino
        uid_liq = uid_venda_liquidacao(
            data_liq=str(data_liq),
            valor_liq=float(valor_liq),
            forma=forma,
            maquineta=maquineta,
            bandeira=bandeira,
            parcelas=parcelas,
            banco=banco_uid,
            usuario=usuario
        )
        if self.mov_repo.ja_existe_transacao(uid_liq):
            return (-1, -1)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # 1) entrada
            cur.execute("""
                INSERT INTO entrada
                    (Data, Valor, Forma_de_Pagamento, Parcelas, Bandeira, Usuario, maquineta, valor_liquido, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(data_venda), float(valor_bruto), forma, parcelas, bandeira, usuario, maquineta, valor_liq,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))
            venda_id = int(cur.lastrowid)

            # 2) saldos na data_liq
            if forma == "DINHEIRO":
                # incrementa caixa_vendas (ajuste acordado)
                self._garantir_linha_saldos_caixas(conn, str(data_liq))
                cur.execute("""
                    UPDATE saldos_caixas SET caixa_vendas = COALESCE(caixa_vendas,0) + ?
                    WHERE data = ?
                """, (float(valor_bruto), str(data_liq)))
                banco_mov = "Caixa"
                valor_mov = valor_liq  # == bruto, pois taxa 0
            else:
                if not banco_destino:
                    raise ValueError("Banco de destino obrigatório para formas não-DINHEIRO.")
                self._ajustar_banco_dynamic(conn, banco_destino, +valor_liq, str(data_liq))
                banco_mov = banco_destino
                valor_mov = valor_liq

            # 3) log único (mesma conexão)
            detalhes = []
            if maquineta: detalhes.append(maquineta)
            if bandeira:  detalhes.append(bandeira)
            if parcelas and parcelas > 1: detalhes.append(f"{parcelas}x")
            detalhes_txt = " ".join(detalhes).strip()
            obs = f"Liquidação {forma}" + (f" {detalhes_txt}" if detalhes_txt else "")

            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (str(data_liq), banco_mov, "entrada", float(valor_mov),
                  "vendas_liquidacao", obs, "entrada", venda_id, uid_liq))
            mov_id = int(cur.lastrowid)

            conn.commit()

        return (venda_id, mov_id)