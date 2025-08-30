import os, sqlite3

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB   = os.path.join(ROOT, "data", "flowdash_data.db")
print("DB:", DB)

con = sqlite3.connect(DB)
cur = con.cursor()

# Verifica se a coluna já existe
cols = [r[1] for r in cur.execute("PRAGMA table_info(contas_a_pagar_mov);").fetchall()]
if "data_pagamento" in cols:
    print("OK: coluna data_pagamento já existe em contas_a_pagar_mov.")
else:
    print("Adicionando coluna data_pagamento em contas_a_pagar_mov ...")
    cur.execute("ALTER TABLE contas_a_pagar_mov ADD COLUMN data_pagamento TEXT;")
    con.commit()
    print("Coluna criada com sucesso.")

con.close()
