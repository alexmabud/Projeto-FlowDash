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
- utils.utils.resolve_db_path
"""

from __future__ import annotations
from typing import Any
import sqlite3
from utils.utils import resolve_db_path


def get_conn(db_path_like: Any) -> sqlite3.Connection:
    """
    Abre uma conexão SQLite pronta para uso em produção.

    Aceita:
        - Caminho (str ou PathLike)
        - Objetos com atributo `db_path`, `caminho_banco` ou `database`
          (ex.: SimpleNamespace, config, etc.)

    Args:
        db_path_like (Any): Referência ao banco (string/PathLike/objeto com atributo de caminho).

    Returns:
        sqlite3.Connection: Conexão aberta. O chamador é responsável por fechá-la.
    """
    db_path = resolve_db_path(db_path_like)

    conn = sqlite3.connect(
        db_path,
        timeout=30,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    # PRAGMAs padrão do projeto
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    # Rows acessíveis por nome de coluna
    conn.row_factory = sqlite3.Row
    return conn


# API pública explícita
__all__ = ["get_conn"]
