# remove_coluna_valor_bruto.py
import sqlite3
import os

# Caminho absoluto do banco conforme sua estrutura
caminho_banco = os.path.join(
    "C:/Users/User/OneDrive/Documentos/Python/Dev_Python/Abud Python Workspace - GitHub/Projeto FlowDash/data",
    "flowdash_data.db"
)

with sqlite3.connect(caminho_banco) as conn:
    cur = conn.cursor()

    # 1. Verifica as colunas atuais
    schema = cur.execute("PRAGMA table_info(entrada);").fetchall()
    colunas = [c[1] for c in schema]
    print("Colunas atuais:", colunas)

    if "Valor_Bruto" not in colunas:
        print("✅ A coluna 'Valor_Bruto' já não existe.")
    else:
        print("⚠️ Removendo a coluna 'Valor_Bruto'...")

        # 2. Desativa foreign_keys temporariamente
        cur.execute("PRAGMA foreign_keys=off;")

        # 3. Cria nova tabela sem a coluna Valor_Bruto
        colunas_sem = [c for c in colunas if c != "Valor_Bruto"]
        cols_sql = ", ".join(colunas_sem)
        cur.execute(f"CREATE TABLE entrada_nova AS SELECT {cols_sql} FROM entrada;")

        # 4. Remove a tabela antiga e renomeia a nova
        cur.execute("DROP TABLE entrada;")
        cur.execute("ALTER TABLE entrada_nova RENAME TO entrada;")

        cur.execute("PRAGMA foreign_keys=on;")
        conn.commit()

        print("✅ Coluna 'Valor_Bruto' removida com sucesso.")
