import sqlite3
from pathlib import Path

DB_PATH = Path(r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db")

SQL = r"""
-- TABELA CENTRAL (eventos)
CREATE TABLE IF NOT EXISTS contas_a_pagar_mov (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  obrigacao_id     INTEGER NOT NULL,
  tipo_obrigacao   TEXT    NOT NULL,   -- 'BOLETO' | 'FATURA_CARTAO' | 'EMPRESTIMO' | 'OUTRO'
  categoria_evento TEXT    NOT NULL,   -- 'LANCAMENTO'|'PAGAMENTO'|'JUROS'|'MULTA'|'DESCONTO'|'AJUSTE'|'CANCELAMENTO'
  data_evento      TEXT    NOT NULL,   -- 'YYYY-MM-DD'
  vencimento       TEXT,
  valor_evento     REAL    NOT NULL,   -- + aumenta dívida; - reduz dívida
  descricao        TEXT,
  credor           TEXT,
  competencia      TEXT,               -- 'YYYY-MM'
  parcela_num      INTEGER,
  parcelas_total   INTEGER,
  forma_pagamento  TEXT,
  origem           TEXT,
  ledger_id        INTEGER,
  usuario          TEXT    NOT NULL,
  created_at       TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
  CHECK (valor_evento <> 0),
  CHECK (categoria_evento IN ('LANCAMENTO','PAGAMENTO','JUROS','MULTA','DESCONTO','AJUSTE','CANCELAMENTO'))
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_cap_obrigacao      ON contas_a_pagar_mov(obrigacao_id);
CREATE INDEX IF NOT EXISTS idx_cap_tipo_venc      ON contas_a_pagar_mov(tipo_obrigacao, vencimento);
CREATE INDEX IF NOT EXISTS idx_cap_categoria_data ON contas_a_pagar_mov(categoria_evento, data_evento);
CREATE INDEX IF NOT EXISTS idx_cap_ledger         ON contas_a_pagar_mov(ledger_id);

-- VIEW: saldo por obrigação
CREATE VIEW IF NOT EXISTS vw_cap_saldos AS
SELECT
  obrigacao_id,
  MAX(tipo_obrigacao) AS tipo_obrigacao,
  MAX(credor)         AS credor,
  MAX(descricao)      AS descricao,
  MAX(competencia)    AS competencia,
  MAX(vencimento)     AS vencimento,
  MAX(parcelas_total) AS parcelas_total,
  SUM(CASE WHEN categoria_evento='LANCAMENTO' THEN valor_evento ELSE 0 END) AS total_lancado,
  SUM(CASE WHEN categoria_evento<>'LANCAMENTO' THEN valor_evento ELSE 0 END) AS ajustes_e_pagamentos,
  SUM(valor_evento) AS saldo_aberto
FROM contas_a_pagar_mov
GROUP BY obrigacao_id;

-- VIEW: itens em aberto + % quitado
CREATE VIEW IF NOT EXISTS vw_cap_em_aberto AS
SELECT
  obrigacao_id, tipo_obrigacao, credor, descricao, competencia, vencimento, parcelas_total,
  total_lancado,
  (-1.0 * ajustes_e_pagamentos) AS total_pago,
  saldo_aberto,
  ROUND(CASE WHEN total_lancado>0
       THEN ( (total_lancado + ajustes_e_pagamentos) * -100.0 / total_lancado ) + 100.0
       ELSE 0 END, 2) AS perc_quitado
FROM vw_cap_saldos
WHERE saldo_aberto > 0.000001;

-- VIEW: resumo mensal (para gráficos e cards)
CREATE VIEW IF NOT EXISTS vw_cap_resumo_mensal AS
SELECT
  tipo_obrigacao,
  COALESCE(competencia, strftime('%Y-%m', vencimento)) AS competencia,
  SUM(CASE WHEN categoria_evento='LANCAMENTO' THEN valor_evento ELSE 0 END) AS total_lancado,
  SUM(CASE WHEN categoria_evento='PAGAMENTO'  THEN -valor_evento ELSE 0 END) AS total_pago,
  SUM(valor_evento) AS saldo_aberto,
  ROUND(
    100.0 * SUM(CASE WHEN categoria_evento='PAGAMENTO' THEN -valor_evento ELSE 0 END)
    / NULLIF(SUM(CASE WHEN categoria_evento='LANCAMENTO' THEN valor_evento ELSE 0 END),0)
  ,2) AS perc_quitado
FROM contas_a_pagar_mov
GROUP BY tipo_obrigacao, COALESCE(competencia, strftime('%Y-%m', vencimento));
"""

def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Banco não encontrado: {DB_PATH}")

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.executescript(SQL)
        conn.commit()

        # Verificação rápida
        cur = conn.cursor()
        def ok(name, typ):
            cur.execute("SELECT 1 FROM sqlite_master WHERE type=? AND name=?;", (typ, name))
            return "OK" if cur.fetchone() else "FALHOU"
        checks = [
            ("contas_a_pagar_mov","table"),
            ("vw_cap_saldos","view"),
            ("vw_cap_em_aberto","view"),
            ("vw_cap_resumo_mensal","view"),
        ]
        print("\nVerificação:")
        for name, typ in checks:
            print(f" - {typ.upper():4s} {name:30s} -> {ok(name, typ)}")

if __name__ == "__main__":
    main()
    print("\n✔ Estrutura criada/atualizada com sucesso.")