import os, sqlite3
from datetime import date, timedelta

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB   = os.path.join(ROOT, "data", "flowdash_data.db")
print("DB:", DB)

con = sqlite3.connect(DB)
cur = con.cursor()

# pega próximo obrigacao_id
row = cur.execute("SELECT COALESCE(MAX(obrigacao_id),0) FROM contas_a_pagar_mov").fetchone()
base_obrig = int(row[0]) + 1

credor = "Banco Demo"
valor  = 100.00
parcelas = 3
hoje = date.today()
venc1 = hoje.replace(day=1)  # 1º dia do mês corrente
desc  = f"{credor} {parcelas}x"

ids = []
for i in range(1, parcelas+1):
    # datas
    ym = (venc1.replace(day=28) + timedelta(days=4))  # truque próx mês
    vcto = (venc1 if i == 1 else (venc1.replace(day=1) + timedelta(days=32*(i-1)))).replace(day=1)
    vcto = vcto.replace(day=1)  # garante início do mês
    competencia = f"{vcto.year:04d}-{vcto.month:02d}"

    cur.execute("""
        INSERT INTO contas_a_pagar_mov
          (obrigacao_id, tipo_obrigacao, categoria_evento, valor_evento,
           data_evento, vencimento, descricao, credor, competencia,
           parcela_num, parcelas_total, status, tipo_origem)
        VALUES (?, 'EMPRESTIMO', 'LANCAMENTO', ?, ?, ?, ?, ?, ?, ?, ?, 'Em aberto', 'EMPRESTIMO')
    """, (
        base_obrig + (i-1),
        float(valor),
        str(hoje),                 # data_evento (contratação/base)
        str(vcto),                 # vencimento
        f"{desc} {i}/{parcelas}",  # descricao
        credor,
        competencia,
        i, parcelas
    ))
    ids.append(cur.lastrowid)

con.commit()
print(f"OK: empréstimo demo criado. obrigacao_id de {base_obrig} a {base_obrig+parcelas-1}")
con.close()
