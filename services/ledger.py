import sqlite3
from typing import Optional, Tuple, List
import pandas as pd

from repository.movimentacoes_repository import MovimentacoesRepository

class LedgerService:
    """
    Serviço para registrar saídas com idempotência (trans_uid):
      - DINHEIRO  -> ajusta saldos_caixas (caixa / caixa_2) e loga movimentacoes_bancarias
      - PIX/DÉBITO-> ajusta saldos_bancos (coluna dinâmica) e loga movimentacoes_bancarias
      - CRÉDITO   -> gera parcelas em fatura_cartao e loga movimentacoes_bancarias (sem mexer em saldos agora)
      - BOLETO    -> gera parcelas em contas_a_pagar e loga movimentacoes_bancarias (sem mexer em saldos agora)
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.mov_repo = MovimentacoesRepository(db_path)  # garante schema do log

    # ----------------- infra -----------------
    def _get_conn(self):
        # Conexão estável (OneDrive): WAL + busy timeout + FKs
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _sanitize(self, s: Optional[str]) -> str:
        return (s or "").strip()

    # UIDs determinísticos (evita duplicidade com cliques repetidos)
    def _mk_uid_dinheiro(self, data, valor, origem, categoria, sub, desc, usuario) -> str:
        return f"DINHEIRO|{data}|{float(valor):.2f}|{self._sanitize(origem)}|{self._sanitize(categoria)}|{self._sanitize(sub)}|{self._sanitize(desc)}|{self._sanitize(usuario)}"

    def _mk_uid_bancaria(self, data, valor, banco, forma, categoria, sub, desc, usuario) -> str:
        return f"BANCARIA|{forma}|{data}|{float(valor):.2f}|{self._sanitize(banco)}|{self._sanitize(categoria)}|{self._sanitize(sub)}|{self._sanitize(desc)}|{self._sanitize(usuario)}"

    def _mk_uid_credito(self, data_compra, valor, parcelas, cartao, categoria, sub, desc, usuario) -> str:
        return f"CREDITO|{self._sanitize(cartao)}|{data_compra}|{float(valor):.2f}|{int(parcelas)}|{self._sanitize(categoria)}|{self._sanitize(sub)}|{self._sanitize(desc)}|{self._sanitize(usuario)}"

    def _mk_uid_boleto(self, data_compra, valor, parcelas, venc1, categoria, sub, desc, usuario) -> str:
        return f"BOLETO|{data_compra}|{float(valor):.2f}|{int(parcelas)}|{venc1}|{self._sanitize(categoria)}|{self._sanitize(sub)}|{self._sanitize(desc)}|{self._sanitize(usuario)}"

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

    # (opcional) cria tabela alvo se não existir — não altera esquema já existente
    def _garantir_schema_contas_pagar(self, conn):
        conn.execute("""
        CREATE TABLE IF NOT EXISTS contas_a_pagar (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        );
        """)
        # índice idempotente (não falha se a coluna não existir ainda)
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cap_venc ON contas_a_pagar(vencimento);")
        except Exception:
            pass
        conn.commit()

    # ----------------- APIs públicas -----------------
    def registrar_saida_dinheiro(
        self, data: str, valor: float, origem_dinheiro: str,
        categoria: Optional[str], sub_categoria: Optional[str], descricao: Optional[str],
        usuario: str, trans_uid: Optional[str] = None
    ) -> Tuple[int, int]:
        """
        DINHEIRO:
          - INSERT em 'saida' (Forma_de_Pagamento='DINHEIRO', Origem_Dinheiro='Caixa'|'Caixa 2')
          - decrementa saldos_caixas (caixa ou caixa_2)
          - loga movimentacoes_bancarias (banco='Caixa'|'Caixa 2', tipo='saida')
        Retorna: (id_saida, id_mov) ou (-1, -1) se já registrada.
        """
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if origem_dinheiro not in ("Caixa", "Caixa 2"):
            raise ValueError("Origem do dinheiro inválida (use 'Caixa' ou 'Caixa 2').")

        # uid determinístico (idempotência)
        trans_uid = trans_uid or self._mk_uid_dinheiro(
            data, valor, origem_dinheiro, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return (-1, -1)

        categoria = self._sanitize(categoria)
        sub_categoria = self._sanitize(sub_categoria)
        descricao = self._sanitize(descricao)
        usuario = self._sanitize(usuario)

        with self._get_conn() as conn:
            cur = conn.cursor()
            self._garantir_linha_saldos_caixas(conn, data)

            # 1) grava saída
            cur.execute("""
                INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                   Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                   Origem_Dinheiro, Banco_Saida)
                VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
            """, (data, categoria, sub_categoria, descricao, float(valor), usuario, origem_dinheiro))
            id_saida = cur.lastrowid

            # 2) ajusta saldos_caixas
            campo = "caixa" if origem_dinheiro == "Caixa" else "caixa_2"
            cur.execute(f"""
                UPDATE saldos_caixas SET {campo} = COALESCE({campo},0) - ?
                WHERE data = ?
            """, (float(valor), data))

            # 3) log (MESMA conexão)
            obs = f"Saída {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (data, origem_dinheiro, "saida", float(valor), "saidas", obs, "saida", id_saida, trans_uid))
            id_mov = cur.lastrowid

            conn.commit()
            return (id_saida, id_mov)

    def registrar_saida_bancaria(
        self, data: str, valor: float, banco_nome: str, forma: str,
        categoria: Optional[str], sub_categoria: Optional[str], descricao: Optional[str],
        usuario: str, trans_uid: Optional[str] = None
    ) -> Tuple[int, int]:
        """
        PIX ou DÉBITO:
          - INSERT em 'saida' com Banco_Saida = banco_nome
          - decrementa saldos_bancos.<banco_nome> no dia
          - loga movimentacoes_bancarias
        """
        if forma not in ("PIX", "DÉBITO"):
            raise ValueError("Forma inválida. Use 'PIX' ou 'DÉBITO'.")
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")

        banco_nome = self._sanitize(banco_nome)
        categoria = self._sanitize(categoria)
        sub_categoria = self._sanitize(sub_categoria)
        descricao = self._sanitize(descricao)
        usuario = self._sanitize(usuario)

        # uid determinístico
        trans_uid = trans_uid or self._mk_uid_bancaria(
            data, valor, banco_nome, forma, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return (-1, -1)

        with self._get_conn() as conn:
            cur = conn.cursor()

            # 1) grava saída
            cur.execute("""
                INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                   Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                   Origem_Dinheiro, Banco_Saida)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
            """, (data, categoria, sub_categoria, descricao, forma, float(valor), usuario, banco_nome))
            id_saida = cur.lastrowid

            # 2) ajusta saldos_bancos (coluna dinâmica)
            self._ajustar_banco_dynamic(conn, banco_nome, -float(valor), data)

            # 3) log (MESMA conexão)
            obs = f"Saída {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (data, banco_nome, "saida", float(valor), "saidas", obs, "saida", id_saida, trans_uid))
            id_mov = cur.lastrowid

            conn.commit()
            return (id_saida, id_mov)

    def registrar_saida_credito(
        self, data_compra: str, valor: float, parcelas: int, cartao_nome: str,
        categoria: Optional[str], sub_categoria: Optional[str], descricao: Optional[str],
        usuario: str, fechamento: int, vencimento: int,
        trans_uid: Optional[str] = None
    ) -> Tuple[List[int], int]:
        """
        CRÉDITO:
          - NÃO grava em 'saida' e NÃO mexe em saldos agora
          - quebra em parcelas e insere em 'fatura_cartao'
          - loga movimentacoes_bancarias só para rastreio
        Retorna: ([ids_fatura], id_mov) ou ([], -1) se já registrada.
        """
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if int(parcelas) < 1:
            raise ValueError("Quantidade de parcelas inválida.")

        cartao_nome = self._sanitize(cartao_nome)
        categoria = self._sanitize(categoria)
        sub_categoria = self._sanitize(sub_categoria)
        descricao = self._sanitize(descricao)
        usuario = self._sanitize(usuario)

        # uid determinístico
        trans_uid = trans_uid or self._mk_uid_credito(
            data_compra, valor, parcelas, cartao_nome, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return ([], -1)

        compra = pd.to_datetime(data_compra)
        base = compra + pd.DateOffset(months=1) if compra.day > int(fechamento) else compra
        valor_parc = round(float(valor) / int(parcelas), 2)

        ids: List[int] = []
        with self._get_conn() as conn:
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
                ids.append(cur.lastrowid)

            # log (MESMA conexão)
            obs = f"Despesa CRÉDITO {cartao_nome} {parcelas}x - {categoria}/{sub_categoria}"
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (str(compra.date()), cartao_nome, "saida", float(valor),
                  "saidas_credito_programada", obs, "fatura_cartao", ids[0] if ids else None, trans_uid))
            id_mov = cur.lastrowid

            conn.commit()
            return (ids, id_mov)

    def registrar_saida_boleto(
        self, data_compra: str, valor: float, parcelas: int, vencimento_primeira: str,
        categoria: Optional[str], sub_categoria: Optional[str], descricao: Optional[str],
        usuario: str, fornecedor: Optional[str] = None, documento: Optional[str] = None,
        trans_uid: Optional[str] = None
    ) -> Tuple[List[int], int]:
        """
        BOLETO:
          - NÃO mexe saldos agora
          - cria parcelas em 'contas_a_pagar' a partir do vencimento da 1ª parcela (mês a mês)
          - loga movimentacoes_bancarias como 'saidas_boleto_programada' (apenas rastreio)
        Retorna: ([ids_cap], id_mov) ou ([], -1) se já registrada.
        """
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if int(parcelas) < 1:
            raise ValueError("Quantidade de parcelas inválida.")

        categoria = self._sanitize(categoria)
        sub_categoria = self._sanitize(sub_categoria)
        descricao = self._sanitize(descricao)
        usuario = self._sanitize(usuario)
        fornecedor = self._sanitize(fornecedor)
        documento = self._sanitize(documento)

        # idempotência
        trans_uid = trans_uid or self._mk_uid_boleto(
            data_compra, valor, parcelas, vencimento_primeira, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return ([], -1)

        compra = pd.to_datetime(data_compra)
        venc1 = pd.to_datetime(vencimento_primeira)
        valor_parc = round(float(valor) / int(parcelas), 2)
        total_calc = round(valor_parc * int(parcelas), 2)
        ajuste = round(float(valor) - total_calc, 2)

        ids: List[int] = []
        with self._get_conn() as conn:
            cur = conn.cursor()
            self._garantir_schema_contas_pagar(conn)

            # Detecta esquema legado que tem coluna 'data' NOT NULL
            cols_cap = [r[1] for r in cur.execute("PRAGMA table_info(contas_a_pagar)").fetchall()]
            has_legacy_data = "data" in cols_cap

            for p in range(1, int(parcelas) + 1):
                vcto = (venc1 + pd.DateOffset(months=p-1)).date()
                vparc = valor_parc + (ajuste if p == int(parcelas) else 0.0)

                if has_legacy_data:
                    # preenche também a coluna 'data' (legado)
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

                ids.append(cur.lastrowid)

            obs = f"Boleto {parcelas}x - {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (str(compra.date()), "Boleto", "saida", float(valor),
                  "saidas_boleto_programada", obs, "contas_a_pagar", ids[0] if ids else None, trans_uid))
            id_mov = cur.lastrowid

            conn.commit()
            return (ids, id_mov)