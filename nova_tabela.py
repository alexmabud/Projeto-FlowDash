import sqlite3

# Caminho do seu banco
caminho_banco = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"

# Colunas que foram criadas erradas e você quer remover
colunas_erradas = ["PrevFat", "PrevRec"]

with sqlite3.connect(caminho_banco) as conn:
    cursor = conn.cursor()

    # 1. Pega todas as colunas da tabela mercadorias
    cursor.execute("PRAGMA table_info(mercadorias);")
    cols = [row[1] for row in cursor.fetchall()]

    # 2. Filtra colunas corretas (removendo as erradas)
    colunas_mantidas = [c for c in cols if c not in colunas_erradas]

    # 3. Monta as colunas para recriação
    colunas_str = ", ".join([f'"{c}"' for c in colunas_mantidas])

    # 4. Renomeia tabela atual
    cursor.execute("ALTER TABLE mercadorias RENAME TO mercadorias_old;")

    # 5. Cria nova tabela com as colunas corretas
    # ⚠️ Ajuste os tipos conforme o schema real
    schema = """
    CREATE TABLE mercadorias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Data TEXT,
        Colecao TEXT,
        Fornecedor TEXT,
        Valor_Mercadoria REAL,
        Frete REAL,
        Forma_Pagamento TEXT,
        Parcelas INTEGER,
        Previsao_Faturamento TEXT,
        Previsao_Recebimento TEXT,
        Faturamento TEXT,
        Recebimento TEXT,
        Valor_Recebido REAL,
        Frete_Cobrado REAL,
        Recebimento_Obs TEXT,
        Numero_Pedido TEXT,
        Numero_NF TEXT
    );
    """
    cursor.execute(schema)

    # 6. Copia os dados da tabela antiga para a nova
    cursor.execute(f"INSERT INTO mercadorias ({colunas_str}) SELECT {colunas_str} FROM mercadorias_old;")

    # 7. Remove a tabela antiga
    cursor.execute("DROP TABLE mercadorias_old;")

    conn.commit()

print("✅ Colunas removidas com sucesso.")
