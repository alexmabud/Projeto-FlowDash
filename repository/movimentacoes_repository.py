import sqlite3
from typing import Optional

class MovimentacoesRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.garantir_schema()  # garante tabela/índices na inicialização

    def _get_conn(self):
        # Conexão padrão do projeto (WAL + busy timeout + FKs)
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def garantir_schema(self):
        with self._get_conn() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS movimentacoes_bancarias (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL,
                banco TEXT NOT NULL,
                tipo TEXT NOT NULL,              -- 'saida', 'entrada', etc.
                valor REAL NOT NULL,
                origem TEXT NOT NULL,
                observacao TEXT,
                referencia_tabela TEXT,
                referencia_id INTEGER,
                trans_uid TEXT UNIQUE
            );
            CREATE INDEX IF NOT EXISTS idx_mov_data ON movimentacoes_bancarias(data);
            CREATE INDEX IF NOT EXISTS idx_mov_banco ON movimentacoes_bancarias(banco);
            """)
            conn.commit()

    def ja_existe_transacao(self, trans_uid: str) -> bool:
        if not trans_uid:
            return False
        with self._get_conn() as conn:
            cur = conn.execute(
                "SELECT 1 FROM movimentacoes_bancarias WHERE trans_uid = ? LIMIT 1",
                (trans_uid,)
            )
            return cur.fetchone() is not None

    def inserir_log(self, data: str, banco: str, tipo: str, valor: float,
                    origem: str, observacao: str, referencia_tabela: Optional[str],
                    referencia_id: Optional[int], trans_uid: Optional[str]) -> int:
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao,
                     referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (data, banco or "", tipo, float(valor), origem, observacao,
                  referencia_tabela, referencia_id, trans_uid))
            conn.commit()
            return cur.lastrowid