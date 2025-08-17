import sqlite3
from datetime import datetime

def criar_tabela_fatura_itens():
    caminho_banco = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"
    with sqlite3.connect(caminho_banco) as conn:
        cur = conn.cursor()

        # cria tabela se não existir
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fatura_cartao_itens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            purchase_uid TEXT NOT NULL,
            cartao TEXT NOT NULL,
            competencia TEXT NOT NULL,         -- formato YYYY-MM
            data_compra TEXT NOT NULL,         -- ISO: YYYY-MM-DD
            descricao_compra TEXT,
            categoria TEXT,
            parcela_num INTEGER NOT NULL,
            parcelas INTEGER NOT NULL,
            valor_parcela REAL NOT NULL,
            usuario TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """)

        # índices para performance
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_fatura_itens_cartao_comp
        ON fatura_cartao_itens (cartao, competencia)
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_fatura_itens_purchase
        ON fatura_cartao_itens (purchase_uid)
        """)

        conn.commit()
        print("✅ Tabela fatura_cartao_itens criada/verificada com sucesso!")

if __name__ == "__main__":
    criar_tabela_fatura_itens()