import os, sqlite3

# Caminho do DB relativo a esta pasta "scripts"
ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.normpath(os.path.join(ROOT, "..", "data", "flowdash_data.db"))

SQL = """
DROP VIEW IF EXISTS vw_cap_saldos;

CREATE VIEW vw_cap_saldos AS
WITH lanc AS (
  SELECT
    obrigacao_id,
    SUM(COALESCE(valor_evento,0))                AS valor_base,
    SUM(COALESCE(valor_pago_acumulado,0))        AS valor_pago_acumulado,
    SUM(COALESCE(juros_pago,0))                  AS juros_pago,
    SUM(COALESCE(multa_paga,0))                  AS multa_paga,
    SUM(COALESCE(desconto_aplicado,0))           AS desconto_aplicado
  FROM contas_a_pagar_mov
  WHERE categoria_evento = 'LANCAMENTO'
  GROUP BY obrigacao_id
)
SELECT
  l.*,
  (l.valor_base + l.juros_pago + l.multa_paga - l.desconto_aplicado - l.valor_pago_acumulado) AS saldo_aberto
FROM lanc l;
"""

def main():
    print("DB:", DB_PATH)
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Banco não encontrado: {DB_PATH}")
    con = sqlite3.connect(DB_PATH)
    try:
        con.executescript(SQL)
        con.commit()
        print("vw_cap_saldos (re)criada com sucesso.")
        rows = con.execute(
            "SELECT obrigacao_id, saldo_aberto FROM vw_cap_saldos ORDER BY obrigacao_id LIMIT 5;"
        ).fetchall()
        print("Amostra:", rows)
    finally:
        con.close()

if __name__ == "__main__":
    main()
