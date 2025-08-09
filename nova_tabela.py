import sqlite3

# Caminho do banco no seu computador
caminho_banco = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"

with sqlite3.connect(caminho_banco) as conn:
    cur = conn.cursor()

    # Apagar tabela antiga
    cur.execute("DROP TABLE IF EXISTS saldos_bancos")

    # Criar nova tabela com apenas as colunas desejadas
    cur.execute("""
        CREATE TABLE saldos_bancos (
            data TEXT PRIMARY KEY,
            Inter REAL DEFAULT 0.0,
            InfinitePay REAL DEFAULT 0.0,
            Bradesco REAL DEFAULT 0.0
        )
    """)

    conn.commit()

print("✅ Tabela 'saldos_bancos' recriada com sucesso!")