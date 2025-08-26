# migrar_mov_bancarias.py
# -*- coding: utf-8 -*-
"""
Migração: adiciona colunas 'usuario' e 'data_hora' na tabela movimentacoes_bancarias,
preenche 'data_hora' e cria índices úteis.

Pode rodar no terminal, VS Code ou Jupyter (ignora o argumento -f do Jupyter).
"""

import sqlite3
from pathlib import Path
import argparse

DEFAULT_DB_PATH = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;", (name,)
    ).fetchone() is not None


def get_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cols = set()
    for cid, name, _type, _nn, _dflt, _pk in conn.execute(f"PRAGMA table_info({table});"):
        cols.add(str(name))
    return cols


def ensure_columns(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "movimentacoes_bancarias"):
        raise RuntimeError("Tabela 'movimentacoes_bancarias' não existe.")

    cols = get_columns(conn, "movimentacoes_bancarias")

    if "usuario" not in cols:
        conn.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "usuario" TEXT;')
        print(" [+] Coluna 'usuario' adicionada.")

    if "data_hora" not in cols:
        conn.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "data_hora" TEXT;')
        print(" [+] Coluna 'data_hora' adicionada.")


def backfill_data_hora(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE movimentacoes_bancarias
           SET data_hora = COALESCE(data_hora, substr(data,1,10) || 'T00:00:00')
         WHERE data_hora IS NULL OR data_hora = '';
        """
    )
    print(" [+] Backfill de 'data_hora' concluído.")


def ensure_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_mov_data        ON movimentacoes_bancarias (data);
        CREATE INDEX IF NOT EXISTS idx_mov_data_hora   ON movimentacoes_bancarias (data_hora);
        CREATE INDEX IF NOT EXISTS idx_mov_usuario     ON movimentacoes_bancarias (usuario);
        CREATE INDEX IF NOT EXISTS idx_mov_origem      ON movimentacoes_bancarias (origem);
        CREATE INDEX IF NOT EXISTS idx_mov_trans_uid   ON movimentacoes_bancarias (trans_uid);
        """
    )
    print(" [+] Índices criados/garantidos.")


def preview(conn: sqlite3.Connection, limit: int = 10) -> None:
    print("\nAmostra (últimos registros):")
    for row in conn.execute(
        """
        SELECT id, data, data_hora, banco, tipo, valor, origem, usuario, observacao
          FROM movimentacoes_bancarias
         ORDER BY id DESC
         LIMIT ?;
        """,
        (limit,),
    ):
        print(row)


def main(db_path: str | None = None) -> None:
    path = Path(db_path or DEFAULT_DB_PATH)
    if not path.exists():
        raise SystemExit(f"❌ Arquivo de banco não encontrado:\n{path}")

    with sqlite3.connect(path.as_posix(), timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys = ON;")

        print(f"➡ Migrando banco: {path}")
        ensure_columns(conn)
        backfill_data_hora(conn)
        ensure_indexes(conn)
        conn.commit()
        preview(conn)

    print("\n✔ Migração concluída com sucesso.")


if __name__ == "__main__":
    # Usa argparse e IGNORA args desconhecidos (ex.: -f=... do Jupyter)
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        "db",
        nargs="?",
        default=DEFAULT_DB_PATH,
        help="Caminho do arquivo .db (opcional).",
    )
    args, _unknown = parser.parse_known_args()
    main(args.db)
