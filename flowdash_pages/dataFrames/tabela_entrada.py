import sqlite3

caminho_banco = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"

def adicionar_coluna_datetime():
    try:
        with sqlite3.connect(caminho_banco) as conn:
            cursor = conn.cursor()

            # Verifica se a coluna já existe
            cursor.execute("PRAGMA table_info(entrada)")
            colunas = [col[1] for col in cursor.fetchall()]
            if "created_at" not in colunas:
                cursor.execute("ALTER TABLE entrada ADD COLUMN created_at TEXT")
                conn.commit()
                print("✅ Coluna 'created_at' adicionada com sucesso.")
            else:
                print("ℹ️ A coluna 'created_at' já existe.")
    except Exception as e:
        print(f"❌ Erro ao alterar tabela: {e}")

if __name__ == "__main__":
    adicionar_coluna_datetime()