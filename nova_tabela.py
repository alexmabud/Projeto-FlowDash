import sqlite3

# Caminho do banco
CAMINHO_BANCO = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"

# Nome da coluna a remover
COLUNA_REMOVER = "teste"

# Conectar ao banco
conn = sqlite3.connect(CAMINHO_BANCO)
cur = conn.cursor()

# Obter a estrutura atual da tabela
cur.execute("PRAGMA table_info(saldos_bancos)")
colunas_info = cur.fetchall()
colunas_existentes = [c[1] for c in colunas_info]

if COLUNA_REMOVER not in colunas_existentes:
    print(f"Coluna '{COLUNA_REMOVER}' não encontrada na tabela saldos_bancos.")
    conn.close()
    exit()

# Criar lista de colunas sem a que será removida
colunas_novas = [c for c in colunas_existentes if c != COLUNA_REMOVER]
colunas_str = ", ".join(colunas_novas)

# Criar tabela temporária
cur.execute(f"""
    CREATE TABLE saldos_bancos_temp AS
    SELECT {colunas_str}
    FROM saldos_bancos
""")

# Apagar tabela original
cur.execute("DROP TABLE saldos_bancos")

# Renomear tabela temporária para o nome original
cur.execute("ALTER TABLE saldos_bancos_temp RENAME TO saldos_bancos")

conn.commit()
conn.close()

print(f"Coluna '{COLUNA_REMOVER}' removida com sucesso da tabela saldos_bancos.")