import sqlite3
from typing import Optional, Tuple, List

class CartoesRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def obter_por_nome(self, nome: str) -> Optional[Tuple[int, int]]:
        """Retorna (fechamento, vencimento) do cartão ou None se não existir."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT fechamento, vencimento FROM cartoes_credito WHERE nome = ? LIMIT 1",
                (nome,)
            ).fetchone()
            return (row[0], row[1]) if row else None

    def listar_nomes(self) -> List[str]:
        """Lista nomes de cartões cadastrados, ordenados alfabeticamente."""
        with self._get_conn() as conn:
            rows = conn.execute("SELECT nome FROM cartoes_credito ORDER BY nome").fetchall()
            return [r[0] for r in rows]