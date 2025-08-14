import sqlite3
import os

def migrate_add_status(caminho_banco: str) -> None:
    """
    Adiciona a coluna 'status' em contas_a_pagar_mov (se não existir),
    faz backfill com 'Em aberto' e cria índices úteis (idempotente).
    """
    if not os.path.isfile(caminho_banco):
        raise FileNotFoundError(f"Arquivo de banco não encontrado: {caminho_banco}")

    conn = sqlite3.connect(caminho_banco, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    cur = conn.cursor()

    # 0) Garante que a tabela existe
    tb = cur.execute("""
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='contas_a_pagar_mov'
        LIMIT 1
    """).fetchone()
    if not tb:
        conn.close()
        raise RuntimeError("Tabela 'contas_a_pagar_mov' não encontrada no banco.")

    # 1) Verifica se a coluna já existe
    cols = [r[1] for r in cur.execute("PRAGMA table_info(contas_a_pagar_mov);").fetchall()]
    if "status" not in cols:
        cur.execute("ALTER TABLE contas_a_pagar_mov ADD COLUMN status TEXT;")

    # 2) Backfill: define 'Em aberto' onde estiver NULL ou vazio
    cur.execute("""
        UPDATE contas_a_pagar_mov
           SET status = 'Em aberto'
         WHERE status IS NULL OR TRIM(status) = ''
    """)

    # 3) Índices (idempotentes)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_capm_status ON contas_a_pagar_mov(status);")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_capm_tipo_credor
        ON contas_a_pagar_mov (tipo_obrigacao, tipo_origem, credor);
    """)

    conn.commit()

    # 4) (Opcional) Resumo
    resumo = cur.execute("""
        SELECT status, COUNT(*) AS qtd
          FROM contas_a_pagar_mov
         GROUP BY status
         ORDER BY qtd DESC
    """).fetchall()
    conn.close()

    print("Migração concluída. Distribuição de status:")
    for s, q in resumo:
        print(f" - {s!r}: {q}")

if __name__ == "__main__":
    caminho = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"
    migrate_add_status(caminho)