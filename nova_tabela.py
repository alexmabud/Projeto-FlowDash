import sqlite3

DB_PATH = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"
TABELA  = "saldos_bancos"
COLUNA  = "Teste"

def has_column(cur, table, col):
    return any(r[1] == col for r in cur.execute(f'PRAGMA table_info("{table}");').fetchall())

def sqlite_version_tuple(conn):
    v = conn.execute("select sqlite_version();").fetchone()[0]
    return tuple(int(x) for x in v.split("."))

def col_def(c):
    # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
    name, ctype, notnull, dflt, pk = c[1], (c[2] or ""), c[3], c[4], c[5]
    parts = [f'"{name}"']
    if ctype: parts.append(ctype)
    if notnull: parts.append("NOT NULL")
    if dflt is not None: parts.append(f"DEFAULT {dflt}")
    if pk: parts.append("PRIMARY KEY")
    return " ".join(parts)

def main():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        if not has_column(cur, TABELA, COLUNA):
            print(f"[ok] Coluna '{COLUNA}' não existe em '{TABELA}'. Nada a fazer.")
            return

        # Tenta caminho direto (SQLite >= 3.35)
        try:
            if sqlite_version_tuple(conn) >= (3, 35, 0):
                cur.execute(f'ALTER TABLE "{TABELA}" DROP COLUMN "{COLUNA}";')
                conn.commit()
                print(f"[ok] Coluna '{COLUNA}' removida com ALTER TABLE.")
                return
        except sqlite3.OperationalError:
            pass  # cai no rebuild

        # Rebuild da tabela (compatibilidade com SQLite antigo)
        info = cur.execute(f'PRAGMA table_info("{TABELA}");').fetchall()
        keep = [c for c in info if c[1] != COLUNA]
        if not keep:
            raise RuntimeError("Não é possível remover a única coluna da tabela.")

        defs = ", ".join(col_def(c) for c in keep)
        cols = ", ".join(f'"{c[1]}"' for c in keep)

        old = f"{TABELA}__old"
        tmp = f"{TABELA}__new"

        # Renomeia, recria e copia dados
        cur.execute(f'ALTER TABLE "{TABELA}" RENAME TO "{old}";')
        cur.execute(f'CREATE TABLE "{tmp}" ({defs});')
        cur.execute(f'INSERT INTO "{tmp}" ({cols}) SELECT {cols} FROM "{old}";')

        # Recria índices que não usam a coluna removida
        idx_rows = cur.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL;",
            (old,)
        ).fetchall()
        for name, sql in idx_rows:
            if COLUNA in (sql or ""):
                continue
            sql_new = sql.replace(f'"{old}"', f'"{tmp}"').replace(old, tmp)
            cur.execute(sql_new)

        # Troca tabelas
        cur.execute(f'DROP TABLE "{old}";')
        cur.execute(f'ALTER TABLE "{tmp}" RENAME TO "{TABELA}";')
        conn.commit()

        print(f"[ok] Coluna '{COLUNA}' removida reconstruindo a tabela '{TABELA}'.")

if __name__ == "__main__":
    main()