import sqlite3
import shutil
import os
from datetime import datetime

# ✅ CAMINHO DO SEU BANCO (Windows)
DB_PATH = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"

# Colunas a remover
COLS_TO_DROP = {"Banco Teste", "Teste"}

def drop_columns_from_saldos_bancos(db_path: str, cols_to_drop: set[str]) -> None:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Banco não encontrado: {db_path}")

    # 1) Backup
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{db_path}.backup_{ts}"
    shutil.copy2(db_path, backup_path)
    print(f"🛟 Backup criado: {backup_path}")

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # 2) Verifica tabela e colunas atuais
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='saldos_bancos';")
        if not cur.fetchone():
            raise RuntimeError("Tabela 'saldos_bancos' não existe no banco.")

        cols_info = cur.execute("PRAGMA table_info(saldos_bancos);").fetchall()
        # cols_info: [ (cid, name, type, notnull, dflt_value, pk), ... ]
        all_cols = [c[1] for c in cols_info]
        if not set(all_cols) & cols_to_drop:
            print("ℹ️ Nenhuma das colunas a remover existe. Nada a fazer.")
            return

        keep = [c for c in cols_info if c[1] not in cols_to_drop]
        if not keep:
            raise RuntimeError("Não é possível remover todas as colunas: nenhuma restaria.")

        # 3) Monta DDL nova preservando tipo, NOT NULL e DEFAULT
        def col_def(c):
            name = c[1]
            ctype = (c[2] or "").strip() or "TEXT"
            notnull = " NOT NULL" if c[3] else ""
            dflt = ""
            if c[4] is not None:
                # PRAGMA retorna valores possivelmente já entre aspas; repassa como está
                dflt = f" DEFAULT {c[4]}"
            pk = " PRIMARY KEY" if c[5] else ""
            return f'"{name}" {ctype}{notnull}{dflt}{pk}'

        keep_col_defs = ", ".join(col_def(c) for c in keep)
        keep_col_names = ", ".join(f'"{c[1]}"' for c in keep)

        # 4) Transação de recriação
        cur.execute("PRAGMA foreign_keys=OFF;")
        try:
            cur.execute("BEGIN;")
            cur.execute("ALTER TABLE saldos_bancos RENAME TO saldos_bancos_old;")
            cur.execute(f"CREATE TABLE saldos_bancos ({keep_col_defs});")
            cur.execute(
                f"INSERT INTO saldos_bancos ({keep_col_names}) "
                f"SELECT {keep_col_names} FROM saldos_bancos_old;"
            )
            cur.execute("DROP TABLE saldos_bancos_old;")
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise
        finally:
            cur.execute("PRAGMA foreign_keys=ON;")

        print(f"✅ Removidas colunas: {', '.join(sorted(cols_to_drop & set(all_cols)))}")
        print(f"📦 Colunas mantidas: {', '.join(c[1] for c in keep)}")

if __name__ == "__main__":
    drop_columns_from_saldos_bancos(DB_PATH, COLS_TO_DROP)