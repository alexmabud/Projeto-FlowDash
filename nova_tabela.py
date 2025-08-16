import sqlite3
from pathlib import Path
from datetime import datetime
import shutil
import sys

# === AJUSTE AQUI: caminho do ARQUIVO .db ===
# Se o arquivo do banco for "flowdash_data.db" dentro da pasta data, use:
DB_FILE = Path(r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db")

SQL_VIEWS = r"""
-- ============================================================
-- 1) View de saldos por obrigação
-- ============================================================
DROP VIEW IF EXISTS vw_cap_saldos;

CREATE VIEW vw_cap_saldos AS
WITH base AS (
  SELECT
    obrigacao_id,
    SUM(CASE WHEN categoria_evento = 'LANCAMENTO' THEN COALESCE(valor_evento,0) ELSE 0 END)                              AS total_lancado,
    SUM(CASE WHEN categoria_evento = 'PAGAMENTO'   THEN -COALESCE(valor_evento,0) ELSE 0 END)                             AS total_pago,
    SUM(CASE WHEN categoria_evento IN ('LANCAMENTO','PAGAMENTO','AJUSTE','JUROS','MULTA','DESCONTO')
             THEN COALESCE(valor_evento,0) ELSE 0 END)                                                                    AS saldo_aberto
  FROM contas_a_pagar_mov
  GROUP BY obrigacao_id
)
SELECT
  obrigacao_id,
  ROUND(COALESCE(total_lancado,0), 2) AS total_lancado,
  ROUND(COALESCE(total_pago,0),     2) AS total_pago,
  ROUND(COALESCE(saldo_aberto,0),   2) AS saldo_aberto,
  CASE
    WHEN COALESCE(total_lancado,0) > 0
      THEN ROUND( (CASE WHEN total_pago / NULLIF(total_lancado,0) > 1.0 THEN 1.0 ELSE total_pago / NULLIF(total_lancado,0) END), 4)
    ELSE 0.0
  END AS perc_quitado
FROM base;

-- ============================================================
-- 2) View "em aberto" (usada pela UI)
-- ============================================================
DROP VIEW IF EXISTS vw_cap_em_aberto;

CREATE VIEW vw_cap_em_aberto AS
WITH
  anchor AS (
    SELECT m.*
    FROM contas_a_pagar_mov m
    JOIN (
      SELECT obrigacao_id, MIN(id) AS min_id
      FROM contas_a_pagar_mov
      WHERE categoria_evento = 'LANCAMENTO'
      GROUP BY obrigacao_id
    ) a ON a.obrigacao_id = m.obrigacao_id AND a.min_id = m.id
  ),
  saldos AS (
    SELECT * FROM vw_cap_saldos
  )
SELECT
  s.obrigacao_id,
  a.tipo_obrigacao,
  a.credor,
  a.descricao,
  a.competencia,
  a.vencimento,
  s.total_lancado,
  s.total_pago,
  s.saldo_aberto,
  s.perc_quitado
FROM saldos s
LEFT JOIN anchor a ON a.obrigacao_id = s.obrigacao_id
WHERE ROUND(COALESCE(s.saldo_aberto,0), 2) > 0.00;

-- (Opcional) índices para performance
CREATE INDEX IF NOT EXISTS idx_capm_obrig ON contas_a_pagar_mov(obrigacao_id);
CREATE INDEX IF NOT EXISTS idx_capm_cat   ON contas_a_pagar_mov(categoria_evento);
CREATE INDEX IF NOT EXISTS idx_capm_tipoo ON contas_a_pagar_mov(tipo_origem);
"""

def backup_db(db_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = db_path.with_suffix(f".bak_{ts}.db")
    shutil.copy2(db_path, bak)
    return bak

def main():
    if not DB_FILE.exists():
        print(f"❌ Arquivo do banco não encontrado:\n{DB_FILE}")
        sys.exit(1)

    print(f"🔧 Migrando views em: {DB_FILE}")
    bak = backup_db(DB_FILE)
    print(f"🗂️  Backup criado: {bak}")

    try:
        with sqlite3.connect(str(DB_FILE)) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.executescript(SQL_VIEWS)
            conn.commit()
        print("✅ Views recriadas com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao recriar views: {e}")
        print("ℹ️  O backup permanece disponível.")
        sys.exit(1)

if __name__ == "__main__":
    main()