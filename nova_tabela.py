import sqlite3

CAMINHO_BANCO = r"C:/Users/User/OneDrive/Documentos/Python/Dev_Python/Abud Python Workspace - GitHub/Projeto FlowDash/data/flowdash_data.db"

with sqlite3.connect(CAMINHO_BANCO) as conn:
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS mercadorias_backup_without_id;")
    conn.commit()

print("✅ Tabela 'mercadorias_backup_without_id' excluída com sucesso!")
