import os, sqlite3

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB   = os.path.join(ROOT, "data", "flowdash_data.db")
print("DB:", DB)

con = sqlite3.connect(DB)
cur = con.cursor()

rows = cur.execute("""
SELECT m.obrigacao_id,
       ROUND(COALESCE(v.saldo_aberto,0), 2) AS saldo
FROM (
  SELECT DISTINCT obrigacao_id
  FROM contas_a_pagar_mov
  WHERE categoria_evento = 'LANCAMENTO'
    AND (tipo_obrigacao='EMPRESTIMO' OR tipo_origem='EMPRESTIMO')
) m
LEFT JOIN vw_cap_saldos v ON v.obrigacao_id = m.obrigacao_id
WHERE COALESCE(v.saldo_aberto,0) > 0.005
ORDER BY m.obrigacao_id;
""").fetchall()

if not rows:
    print("Nenhum EMPRESTIMO em aberto encontrado.")
else:
    print("EMPRESTIMO(s) em aberto:")
    for oid, saldo in rows:
        print(f"  obrigacao_id={oid}  saldo={saldo:.2f}")

con.close()
