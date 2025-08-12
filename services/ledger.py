import pandas as pd
import sqlite3
from typing import Optional, Tuple, List
from datetime import date

from shared.db import get_conn
from shared.ids import (
    sanitize,
    uid_saida_dinheiro,
    uid_saida_bancaria,
    uid_credito_programado,
    uid_boleto_programado,
)
from repository.movimentacoes_repository import MovimentacoesRepository

class LedgerService:
    """
    Serviço para registrar saídas com idempotência:
      - DINHEIRO  -> ajusta saldos_caixas (caixa / caixa_2) e loga movimentacoes_bancarias
      - PIX/DÉBITO-> ajusta saldos_bancos (coluna dinâmica) e loga movimentacoes_bancarias
      - CRÉDITO   -> gera parcelas em fatura_cartao e loga movimentacoes_bancarias (sem mexer em saldos agora)
      - BOLETO    -> gera parcelas em contas_a_pagar e loga movimentacoes_bancarias (sem mexer em saldos agora)
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.mov_repo = MovimentacoesRepository(db_path)

    # ---------- infra ----------
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
        cols = pd.read_sql("PRAGMA table_info(saldos_bancos);", conn)["name"].tolist()
        if banco_col not in cols:
            conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_col}" REAL DEFAULT 0.0;')
        self._garantir_linha_saldos_bancos(conn, data)
        conn.execute(
            f'UPDATE saldos_bancos SET "{banco_col}" = COALESCE("{banco_col}",0) + ? WHERE data = ?',
            (float(delta), data)
        )

    @staticmethod
    def _add_months(dt: date, months: int) -> date:
        y = dt.year + (dt.month - 1 + months) // 12
        m = (dt.month - 1 + months) % 12 + 1
        d = min(dt.day, [31,
                         29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28,
                         31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
        return date(y, m, d)

    # ---------- DINHEIRO ----------
    def registrar_saida_dinheiro(
        self,
        *,
        data: str,
        valor: float,
        origem_dinheiro: str,      # "Caixa" ou "Caixa 2"
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        trans_uid: Optional[str] = None
    ) -> Tuple[int, int]:
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if origem_dinheiro not in ("Caixa", "Caixa 2"):
            raise ValueError("Origem do dinheiro inválida (use 'Caixa' ou 'Caixa 2').")

        categoria  = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao  = sanitize(descricao)
        usuario    = sanitize(usuario)

        trans_uid = trans_uid or uid_saida_dinheiro(
            data, valor, origem_dinheiro, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return (-1, -1)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()
            self._garantir_linha_saldos_caixas(conn, data)

            cur.execute("""
                INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                   Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                   Origem_Dinheiro, Banco_Saida)
                VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
            """, (data, categoria, sub_categoria, descricao, float(valor), usuario, origem_dinheiro))
            id_saida = int(cur.lastrowid)

            campo = "caixa" if origem_dinheiro == "Caixa" else "caixa_2"
            cur.execute(f"""
                UPDATE saldos_caixas SET {campo} = COALESCE({campo},0) - ?
                WHERE data = ?
            """, (float(valor), data))

            obs = f"Saída {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'saida', ?, 'saidas', ?, 'saida', ?, ?)
            """, (data, origem_dinheiro, float(valor), obs, id_saida, trans_uid))
            id_mov = int(cur.lastrowid)

            conn.commit()
            return (id_saida, id_mov)

    # ---------- PIX / DÉBITO ----------
    def registrar_saida_bancaria(
        self,
        *,
        data: str,
        valor: float,
        banco_nome: str,
        forma: str,               # "PIX" ou "DÉBITO"
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        trans_uid: Optional[str] = None
    ) -> Tuple[int, int]:
        forma_u = sanitize(forma).upper()
        if forma_u == "DEBITO":
            forma_u = "DÉBITO"
        if forma_u not in ("PIX", "DÉBITO"):
            raise ValueError("Forma inválida para saída bancária.")
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")

        banco_nome = sanitize(banco_nome)
        categoria  = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao  = sanitize(descricao)
        usuario    = sanitize(usuario)

        trans_uid = trans_uid or uid_saida_bancaria(
            data, valor, banco_nome, forma_u, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return (-1, -1)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                   Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                   Origem_Dinheiro, Banco_Saida)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
            """, (data, categoria, sub_categoria, descricao, forma_u, float(valor), usuario, banco_nome))
            id_saida = int(cur.lastrowid)

            self._garantir_linha_saldos_bancos(conn, data)
            self._ajustar_banco_dynamic(conn, banco_nome, -float(valor), data)

            obs = f"Saída {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'saida', ?, 'saidas', ?, 'saida', ?, ?)
            """, (data, banco_nome, float(valor), obs, id_saida, trans_uid))
            id_mov = int(cur.lastrowid)

            conn.commit()
            return (id_saida, id_mov)

    # ---------- CRÉDITO ----------
    def registrar_saida_credito(
        self,
        *,
        data_compra: str,          # YYYY-MM-DD
        valor: float,
        parcelas: int,
        cartao_nome: str,
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        fechamento: int,           # dia do mês
        vencimento: int,           # dia do mês
        trans_uid: Optional[str] = None
    ) -> Tuple[List[int], int]:
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if int(parcelas) < 1:
            raise ValueError("Quantidade de parcelas inválida.")

        cartao_nome = sanitize(cartao_nome)
        categoria  = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao  = sanitize(descricao)
        usuario    = sanitize(usuario)

        trans_uid = trans_uid or uid_credito_programado(
            data_compra, valor, parcelas, cartao_nome, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return ([], -1)

        compra = pd.to_datetime(data_compra)
        base = (compra + pd.DateOffset(months=1)) if compra.day > int(fechamento) else compra
        valor_parc = round(float(valor) / int(parcelas), 2)

        ids: List[int] = []
        with get_conn(self.db_path) as conn:
            cur = conn.cursor()
            for p in range(1, int(parcelas) + 1):
                vcto = base.replace(day=int(vencimento)) + pd.DateOffset(months=p-1)
                cur.execute("""
                    INSERT INTO fatura_cartao
                        (data, vencimento, cartao, parcela, total_parcelas, valor,
                         categoria, sub_categoria, descricao, usuario)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (str(compra.date()), str(vcto.date()), cartao_nome, p, int(parcelas),
                      valor_parc, categoria, sub_categoria, descricao, usuario))
                ids.append(int(cur.lastrowid))

            obs = f"Despesa CRÉDITO {cartao_nome} {parcelas}x - {categoria}/{sub_categoria}"
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'saida', ?, 'saidas_credito_programada', ?, 'fatura_cartao', ?, ?)
            """, (str(compra.date()), cartao_nome, float(valor), obs, ids[0] if ids else None, trans_uid))
            id_mov = int(cur.lastrowid)

            conn.commit()
            return (ids, id_mov)

    # ---------- BOLETO ----------
    def registrar_saida_boleto(
        self,
        *,
        data_compra: str,
        valor: float,
        parcelas: int,
        vencimento_primeira: str,   # YYYY-MM-DD
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        fornecedor: Optional[str],
        documento: Optional[str],
        trans_uid: Optional[str] = None
    ) -> Tuple[List[int], int]:
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if int(parcelas) < 1:
            raise ValueError("Quantidade de parcelas inválida.")

        categoria  = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao  = sanitize(descricao)
        usuario    = sanitize(usuario)
        fornecedor = sanitize(fornecedor)
        documento  = sanitize(documento)

        trans_uid = trans_uid or uid_boleto_programado(
            data_compra, valor, parcelas, vencimento_primeira, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return ([], -1)

        compra = pd.to_datetime(data_compra)
        venc1  = pd.to_datetime(vencimento_primeira)
        valor_parc = round(float(valor) / int(parcelas), 2)
        total_calc = round(valor_parc * int(parcelas), 2)
        ajuste = round(float(valor) - total_calc, 2)

        ids: List[int] = []
        with get_conn(self.db_path) as conn:
            cur = conn.cursor()
            # Detecta esquema legado com coluna 'data' (NOT NULL)
            cols_cap = [r[1] for r in cur.execute("PRAGMA table_info(contas_a_pagar)").fetchall()]
            has_legacy_data = "data" in cols_cap

            for p in range(1, int(parcelas) + 1):
                vcto = (venc1 + pd.DateOffset(months=p-1)).date()
                vparc = valor_parc + (ajuste if p == int(parcelas) else 0.0)

                if has_legacy_data:
                    cur.execute("""
                        INSERT INTO contas_a_pagar
                            (data, data_lanc, vencimento, fornecedor, documento,
                             parcela, total_parcelas, valor, categoria, sub_categoria, descricao, usuario, pago)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """, (str(compra.date()), str(compra.date()), str(vcto), fornecedor, documento,
                          p, int(parcelas), float(vparc), categoria, sub_categoria, descricao, usuario))
                else:
                    cur.execute("""
                        INSERT INTO contas_a_pagar
                            (data_lanc, vencimento, fornecedor, documento,
                             parcela, total_parcelas, valor, categoria, sub_categoria, descricao, usuario, pago)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """, (str(compra.date()), str(vcto), fornecedor, documento,
                          p, int(parcelas), float(vparc), categoria, sub_categoria, descricao, usuario))

                ids.append(int(cur.lastrowid))

            obs = f"Boleto {parcelas}x - {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, 'Boleto', 'saida', ?, 'saidas_boleto_programada', ?, 'contas_a_pagar', ?, ?)
            """, (str(compra.date()), float(valor), obs, ids[0] if ids else None, trans_uid))
            id_mov = int(cur.lastrowid)

            conn.commit()
            return (ids, id_mov)