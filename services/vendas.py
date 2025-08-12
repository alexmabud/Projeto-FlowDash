import sqlite3
from typing import Optional, Tuple
import pandas as pd
from repository.movimentacoes_repository import MovimentacoesRepository

class VendasService:
    """
    Regras de negócio para registrar vendas (entrada + liquidação + saldos + log idempotente).
    A UI calcula data_liq e decide forma/maquineta/bandeira/parcelas/taxa/banco_destino.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.mov_repo = MovimentacoesRepository(db_path)  # garante schema

    # ---------- infra ----------
    def _get_conn(self):
        # Conexão estável p/ OneDrive: WAL + busy_timeout + FKs
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _sanitize(self, s: Optional[str]) -> str:
        return (s or "").strip()

    def _garantir_linha_saldos_caixas(self, conn, data: str):
        cur = conn.execute("SELECT 1 FROM saldos_caixas WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("""
                INSERT INTO saldos_caixas (data, caixa, caixa_2, caixa_vendas, caixa2_dia, caixa_total, caixa2_total)
                VALUES (?, 0, 0, 0, 0, 0, 0)
            """, (data,))

    def _garantir_linha_saldos_bancos(self, conn, data: str):
        cur = conn.execute("SELECT 1 FROM saldos_bancos WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("INSERT OR IGNORE INTO saldos_bancos (data) VALUES (?)", (data,))

    def _ajustar_banco_dynamic(self, conn, banco_col: str, delta: float, data: str):
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

    # ---------- regra principal ----------
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
        """
        Retorna: (venda_id, mov_id) ou (-1, -1) se idempotência bloquear.
        Faz:
          - INSERT em 'entrada' (bruto + líquido)
          - Atualiza saldos na data_liq (caixa_vendas ou banco)
          - 1 log (liquidação) idempotente em movimentacoes_bancarias
        """
        # validações
        if float(valor_bruto) <= 0:
            raise ValueError("Valor da venda deve ser maior que zero.")
        forma = self._sanitize(forma).upper()
        if forma not in ("DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"):
            raise ValueError("Forma de pagamento inválida.")

        parcelas = int(parcelas or 1)
        bandeira = self._sanitize(bandeira)
        maquineta = self._sanitize(maquineta)
        usuario = self._sanitize(usuario)
        banco_destino = self._sanitize(banco_destino) if banco_destino else ""

        # valor líquido
        valor_liq = round(float(valor_bruto) * (1 - float(taxa_percentual or 0.0)/100.0), 2)

        # idempotência (1 único movimento: LIQUIDAÇÃO)
        uid_banco = "Caixa" if forma == "DINHEIRO" else banco_destino
        uid_liq = f"VENDA_LIQ|{data_liq}|{valor_liq:.2f}|{forma}|{maquineta}|{bandeira}|{parcelas}|{uid_banco}|{usuario}"
        if self.mov_repo.ja_existe_transacao(uid_liq):
            return (-1, -1)

        with self._get_conn() as conn:
            cur = conn.cursor()

            # 1) entrada
            cur.execute("""
                INSERT INTO entrada
                    (Data, Valor, Forma_de_Pagamento, Parcelas, Bandeira, Usuario, maquineta, valor_liquido, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data_venda, float(valor_bruto), forma, parcelas, bandeira, usuario, maquineta, valor_liq,
                pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
            ))
            venda_id = cur.lastrowid

            # 2) saldos na data_liq
            if forma == "DINHEIRO":
                # incrementa caixa_vendas (ajuste acordado)
                self._garantir_linha_saldos_caixas(conn, data_liq)
                cur.execute("""
                    UPDATE saldos_caixas SET caixa_vendas = COALESCE(caixa_vendas,0) + ?
                    WHERE data = ?
                """, (float(valor_bruto), data_liq))
                banco_mov = "Caixa"
                valor_mov = valor_liq  # == bruto, pois taxa 0
            else:
                if not banco_destino:
                    raise ValueError("Banco de destino obrigatório para formas não-DINHEIRO.")
                self._ajustar_banco_dynamic(conn, banco_destino, +valor_liq, data_liq)
                banco_mov = banco_destino
                valor_mov = valor_liq

            # 3) log único (mesma conexão)
            obs = (f"Liquidação {forma} {maquineta}" + (f"/{bandeira}" if bandeira else "") + (f" {parcelas}x" if parcelas > 1 else "")).strip()
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (data_liq, banco_mov, "entrada", float(valor_mov),
                  "vendas_liquidacao", obs, "entrada", venda_id, uid_liq))
            mov_id = cur.lastrowid

            conn.commit()

        return (venda_id, mov_id)