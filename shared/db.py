# shared/db.py
import sqlite3

def get_conn(db_path: str) -> sqlite3.Connection:
    """
    Abre conexão SQLite com configurações seguras e performáticas:
      - WAL: permite concorrência entre leitura e escrita
      - busy_timeout: espera até 30s para evitar 'database is locked'
      - foreign_keys: garante integridade referencial
      - synchronous=NORMAL: equilíbrio entre segurança e performance
      - row_factory: permite acesso às colunas por nome
      - detect_types: habilita parsing automático de DATE/DATETIME
    """
    conn = sqlite3.connect(
        db_path,
        timeout=30,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.row_factory = sqlite3.Row
    return conn