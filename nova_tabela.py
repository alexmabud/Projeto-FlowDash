import sqlite3

caminho_banco = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"

with sqlite3.connect(caminho_banco) as conn:
    conn.execute("ALTER TABLE movimentacoes_bancarias ADD COLUMN referencia_id INTEGER")
    conn.commit()

print("Coluna 'referencia_id' adicionada com sucesso.")