# flowdash_pages/dataframes/dataframes.py
from __future__ import annotations
import sqlite3
from typing import Iterable, Optional, Sequence

import pandas as pd


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1;",
        (table,),
    )
    return cur.fetchone() is not None


def get_dataframe(
    caminho_banco: str,
    table: str,
    columns: Optional[Sequence[str]] = None,
    where: Optional[str] = None,
    params: Optional[Iterable] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Lê uma tabela do SQLite em DataFrame, com filtros opcionais.

    Args:
        caminho_banco: caminho do arquivo .db (SQLite).
        table: nome da tabela (ex.: 'entradas', 'saidas', 'mercadorias').
        columns: colunas a selecionar; se None, usa '*'.
        where: cláusula WHERE (sem a palavra WHERE), ex.: "data = ? AND banco = ?".
        params: parâmetros para o WHERE (iterável; ex.: [data, banco]).
        order_by: cláusula ORDER BY (sem a palavra ORDER BY), ex.: "data DESC, id DESC".
        limit: limitar quantidade de linhas.

    Returns:
        pandas.DataFrame (vazio se a tabela não existir).
    """
    if not isinstance(table, str) or not table.strip():
        raise ValueError("get_dataframe: 'table' deve ser uma string não vazia.")

    cols_sql = "*"
    if columns:
        safe_cols = [c for c in columns if isinstance(c, str) and c.strip()]
        cols_sql = ", ".join([f'"{c}"' for c in safe_cols]) if safe_cols else "*"

    sql_parts = [f'SELECT {cols_sql} FROM "{table.strip()}"']
    if where and where.strip():
        sql_parts.append(f"WHERE {where.strip()}")
    if order_by and order_by.strip():
        sql_parts.append(f"ORDER BY {order_by.strip()}")
    if isinstance(limit, int) and limit > 0:
        sql_parts.append(f"LIMIT {limit}")

    final_sql = " ".join(sql_parts)
    params = tuple(params) if params is not None else ()

    with sqlite3.connect(caminho_banco, timeout=30) as conn:
        conn.execute("PRAGMA foreign_keys=ON;")
        if not _table_exists(conn, table.strip()):
            # retorna DF vazio (com colunas pedidas, se houver)
            return pd.DataFrame(columns=list(columns) if columns else [])
        df = pd.read_sql(final_sql, conn, params=params)

    return df
