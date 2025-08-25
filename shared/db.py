"""
Módulo DB (Shared)
==================

Camada de acesso SQLite (PRAGMAs padrão) para o FlowDash.

Funcionalidades principais
--------------------------
- Fornece uma função utilitária `get_conn` para abrir conexões SQLite já
  configuradas para uso em produção.
- Configuração automática de PRAGMAs de integridade e performance.
- Suporte a parsing automático de DATE/DATETIME.
- Retorno de resultados com `row_factory` permitindo acesso por nome de coluna.

Detalhes técnicos
-----------------
- `journal_mode = WAL`: permite concorrência de leitura/escrita.
- `busy_timeout = 30000 ms`: evita erros de *database is locked*.
- `foreign_keys = ON`: garante integridade referencial.
- `synchronous = NORMAL`: equilíbrio entre segurança e performance.
- `row_factory = sqlite3.Row`: acesso às colunas por nome.
- `detect_types = PARSE_DECLTYPES | PARSE_COLNAMES`: parsing de DATE/DATETIME.

Dependências
------------
- sqlite3
"""

import sqlite3


def get_conn(db_path: str) -> sqlite3.Connection:
    """
    Abre uma conexão SQLite pronta para uso em produção.

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


# API pública explícita
__all__ = ["get_conn"]
