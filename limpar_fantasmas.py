import sqlite3

# Caminho do banco
caminho_banco = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"

with sqlite3.connect(caminho_banco) as conn:
    cur = conn.cursor()

    # Contar antes
    cur.execute("""
        SELECT COUNT(*) 
        FROM contas_a_pagar_mov 
        WHERE COALESCE(valor_evento,0) = 0
          AND UPPER(COALESCE(status,'EM ABERTO')) IN ('EM ABERTO','PARCIAL')
    """)
    qtd_antes = cur.fetchone()[0]
    print(f"Antes da limpeza: {qtd_antes} registros-lixo encontrados.")

    # Deletar fantasmas
    cur.execute("""
        DELETE FROM contas_a_pagar_mov 
        WHERE COALESCE(valor_evento,0) = 0
          AND UPPER(COALESCE(status,'EM ABERTO')) IN ('EM ABERTO','PARCIAL')
    """)
    conn.commit()

    # Contar depois
    cur.execute("""
        SELECT COUNT(*) 
        FROM contas_a_pagar_mov 
        WHERE COALESCE(valor_evento,0) = 0
          AND UPPER(COALESCE(status,'EM ABERTO')) IN ('EM ABERTO','PARCIAL')
    """)
    qtd_depois = cur.fetchone()[0]
    print(f"Depois da limpeza: {qtd_depois} registros-lixo restantes.")
