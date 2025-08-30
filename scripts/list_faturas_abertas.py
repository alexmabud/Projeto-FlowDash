
import os, sqlite3

# Caminho do DB a partir da pasta /scripts
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(ROOT, 'data', 'flowdash_data.db')

con = sqlite3.connect(DB_PATH)
con.row_factory = sqlite3.Row
cur = con.cursor()

sql = """
SELECT v.obrigacao_id,
       ROUND(v.saldo_aberto, 2) AS saldo_aberto
FROM vw_cap_saldos v
JOIN contas_a_pagar_mov m
  ON m.obrigacao_id = v.obrigacao_id
 AND m.categoria_evento = 'LANCAMENTO'
WHERE (m.tipo_obrigacao='FATURA_CARTAO' OR m.tipo_origem='FATURA_CARTAO')
  AND COALESCE(m.status,'') <> 'Quitado'
  AND v.saldo_aberto > 0.005
GROUP BY v.obrigacao_id
ORDER BY v.obrigacao_id;
"""

rows = cur.execute(sql).fetchall()
print(f"DB: {DB_PATH}")
print("FATURA_CARTAO em aberto:")
if not rows:
    print("  (nenhuma)")
else:
    for r in rows:
        print(f"  obrigacao_id={r['obrigacao_id']}  saldo={r['saldo_aberto']:.2f}")

con.close()

