# check_cap_snapshot.py
from __future__ import annotations
import os, sqlite3, datetime as dt

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "flowdash_data.db")

TARGET_TABLES_HINTS = [
    "contas_a_pagar", "contas_a_pagar_mov",
    "cap", "cap_mov", "lancamento", "lancamentos",
    "saida", "movimentacoes_bancarias",
]

def find_tables(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    names = [r[0] for r in cur.fetchall()]
    # prioriza tabelas “alvo”
    ordered = sorted(names, key=lambda n: (0 if any(h in n.lower() for h in TARGET_TABLES_HINTS) else 1, n))
    return ordered

def guess_order_col(conn, table):
    # tenta achar coluna 'id' ou similar; senão usa rowid
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    for cand in ("id", "ID", "Id", "id_mov", "id_saida"):
        if cand in cols:
            return cand
    return "rowid"

def preview_table(conn, table, limit=10):
    order_col = guess_order_col(conn, table)
    sql = f'SELECT * FROM "{table}" ORDER BY {order_col} DESC LIMIT {limit}'
    try:
        cur = conn.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return cols, rows
    except Exception as e:
        return [], [("ERRO AO LER", str(e))]

def main():
    print("DB:", DB_PATH)
    if not os.path.exists(DB_PATH):
        print("❌ Banco não encontrado.")
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        print("\n== Tabelas ==")
        tables = find_tables(conn)
        for t in tables:
            cur = conn.execute(f'SELECT COUNT(*) FROM "{t}"')
            total = cur.fetchone()[0]
            print(f"- {t}: {total} linhas")

        print("\n== Prévia (últimos registros) ==")
        for t in tables:
            if any(h in t.lower() for h in TARGET_TABLES_HINTS):
                print(f"\n--- {t} ---")
                cols, rows = preview_table(conn, t, limit=10)
                if cols:
                    print(" | ".join(cols))
                for r in rows:
                    if isinstance(r, sqlite3.Row):
                        print(" | ".join(str(r[c]) for c in cols))
                    else:
                        print(r)

if __name__ == "__main__":
    main()
