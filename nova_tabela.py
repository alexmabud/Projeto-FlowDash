import sqlite3, os, sys

# >>> Caminho padrão do seu banco (ajuste se precisar)
DB_PATH = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"

def run(db_path: str):
    if not os.path.isfile(db_path):
        print(f"Arquivo não encontrado:\n  {db_path}\n\nVerifique o caminho acima.")
        sys.exit(1)

    conn = sqlite3.connect(db_path, timeout=30)
    # boas práticas p/ OneDrive
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS categorias_saida (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT UNIQUE NOT NULL
    );
    CREATE TABLE IF NOT EXISTS subcategorias_saida (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        categoria_id INTEGER NOT NULL,
        nome         TEXT NOT NULL,
        UNIQUE(categoria_id, nome),
        FOREIGN KEY(categoria_id) REFERENCES categorias_saida(id) ON DELETE CASCADE
    );
    """)
    conn.commit()
    conn.close()
    print(f"OK ✅  Tabelas garantidas em:\n  {db_path}")

if __name__ == "__main__":
    # Se você quiser, pode passar outro caminho como argumento:
    #   python nova_tabela.py "C:\...\seu_banco.db"
    args = [a for a in sys.argv[1:] if not a.startswith('-')]
    run(args[0] if args else DB_PATH)