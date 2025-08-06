import sqlite3
# Caminho do banco de dados (ajustado para o que o usuário informou)
caminho_banco = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"

# Conectar e criar a nova tabela de pagamentos_emprestimos
try:
    with sqlite3.connect(caminho_banco) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pagamentos_emprestimos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_emprestimo INTEGER NOT NULL,
                data_pagamento TEXT NOT NULL,
                valor_pago REAL NOT NULL,
                observacao TEXT,
                FOREIGN KEY (id_emprestimo) REFERENCES emprestimos_financiamentos(id)
            )
        """)
        conn.commit()
        resultado = "✅ Tabela 'pagamentos_emprestimos' criada com sucesso!"
except Exception as e:
    resultado = f"❌ Erro ao criar tabela: {e}"

resultado