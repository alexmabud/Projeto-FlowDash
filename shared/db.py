"""
Camada de acesso SQLite (PRAGMAs padrão) para o FlowDash.

Resumo
------
Fornece uma função utilitária para abrir conexões SQLite já configuradas com
PRAGMAs seguros e performáticos (WAL, busy_timeout, foreign_keys, synchronous),
além de `row_factory` por nome e detecção automática de tipos.

Estilo
------
Docstrings padronizadas no estilo Google (pt-BR).
"""

import sqlite3

def get_conn(db_path: str) -> sqlite3.Connection:
    """Abre uma conexão SQLite pronta para uso em produção.

    Configurações aplicadas:
      - `journal_mode = WAL`: permite concorrência leitura/escrita.
      - `busy_timeout = 30000 ms`: evita erros de *database is locked*.
      - `foreign_keys = ON`: garante integridade referencial.
      - `synchronous = NORMAL`: equilíbrio entre segurança e performance.
      - `row_factory = sqlite3.Row`: acesso às colunas por nome.
      - `detect_types = PARSE_DECLTYPES | PARSE_COLNAMES`: parsing de DATE/DATETIME.

    Args:
        db_path (str): Caminho do arquivo SQLite (.db).

    Returns:
        sqlite3.Connection: Conexão aberta. O chamador é responsável por fechá-la.
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