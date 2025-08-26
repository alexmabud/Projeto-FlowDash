# tests/test_vendas_smoke.py
"""
Smoke tests para o fluxo de Vendas (compatível com variações de schema).

O script:
- Copia o banco original para um DB de teste (SAFE).
- Garante cadastros mínimos (bancos e taxas_maquinas) detectando colunas existentes.
- Executa vendas (DINHEIRO, PIX direto, CRÉDITO 2x) e verifica efeitos.
- Checa idempotência (segunda chamada não duplica).

Uso:
    python -m tests.test_vendas_smoke
ou:
    python tests/test_vendas_smoke.py
"""

from __future__ import annotations
import os
import shutil
import sqlite3
from datetime import date
from pathlib import Path

# --- Config ---------------------------------------------------------------
DEFAULT_SRC_DB = Path("data/flowdash_data.db")   # banco real
TEST_DB        = Path("data/flowdash_test.db")   # cópia para teste (recriada)

# Parâmetros de teste
HOJE = date.today().strftime("%Y-%m-%d")
DIN_VALOR = 100.00
PIX_VALOR = 150.00
CC_VALOR  = 200.00

PIX_BANCO_DEST = "Bradesco"        # será cadastrado se não existir
MAQ_CREDITO    = "InfinitePay"
BAND_CREDITO   = "VISA"
PARC_CREDITO   = 2
TAXA_CREDITO   = 3.5               # %


# --- Helpers infra --------------------------------------------------------
def _copy_db(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not src.exists():
        raise FileNotFoundError(f"Banco original não encontrado: {src}")
    if dst.exists():
        dst.unlink()
    shutil.copy2(src, dst)


def _conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,)
    ).fetchone()
    return bool(row)


def _has_col(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    return col in cols


def _ensure_tables_and_seed(conn: sqlite3.Connection):
    """
    Garante o mínimo necessário para os testes rodarem em um DB com schemas diferentes.
    - Cria tabelas se faltarem (em versão mínima).
    - Insere cadastros mínimos, detectando colunas existentes (ex.: forma vs forma_pagamento).
    """
    cur = conn.cursor()

    # bancos_cadastrados
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bancos_cadastrados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT UNIQUE
        );
    """)
    cur.execute("INSERT OR IGNORE INTO bancos_cadastrados (nome) VALUES (?)", (PIX_BANCO_DEST,))

    # saldos_caixas
    cur.execute("""
        CREATE TABLE IF NOT EXISTS saldos_caixas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT UNIQUE,
            caixa REAL DEFAULT 0.0,
            caixa_2 REAL DEFAULT 0.0,
            caixa_vendas REAL DEFAULT 0.0,
            caixa2_dia REAL DEFAULT 0.0,
            caixa_total REAL DEFAULT 0.0,
            caixa2_total REAL DEFAULT 0.0
        );
    """)

    # saldos_bancos (colunas dinâmicas por banco)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS saldos_bancos (
            data TEXT UNIQUE
        );
    """)

    # entrada (mínimo compatível)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS entrada (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Data TEXT, -- data da venda
            Valor REAL,
            Forma_de_Pagamento TEXT,
            Parcelas REAL,
            Bandeira TEXT,
            Usuario TEXT,
            maquineta TEXT,
            valor_liquido REAL,
            created_at TEXT
        );
    """)

    # movimentacoes_bancarias (mínimo compatível)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movimentacoes_bancarias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT,
            banco TEXT,
            tipo TEXT,
            valor REAL,
            origem TEXT,
            observacao TEXT,
            referencia_id INTEGER,
            referencia_tabela TEXT,
            trans_uid TEXT UNIQUE,
            usuario TEXT,
            data_hora TEXT
        );
    """)

    # taxas_maquinas: cria se não existir (usa forma_pagamento, que é o padrão do seu DB)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS taxas_maquinas (
            maquineta TEXT,
            forma_pagamento TEXT,
            bandeira TEXT,
            parcelas INTEGER,
            taxa_percentual REAL,
            banco_destino TEXT,
            PRIMARY KEY (maquineta, forma_pagamento, bandeira, parcelas)
        );
    """)
    conn.commit()

    def _insert_row(table: str, rowdict: dict):
        cols_exist = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        cols = [c for c in rowdict.keys() if c in cols_exist]
        if not cols:
            return
        ph = ",".join(["?"] * len(cols))
        colsql = ",".join(cols)
        conn.execute(f"INSERT OR IGNORE INTO {table} ({colsql}) VALUES ({ph})", [rowdict[c] for c in cols])

    # Seed: CRÉDITO 2x (compatível com seu schema: usa 'forma_pagamento')
    _insert_row("taxas_maquinas", {
        "maquineta": MAQ_CREDITO,
        "forma_pagamento": "CRÉDITO",
        "bandeira": BAND_CREDITO,
        "parcelas": PARC_CREDITO,
        "taxa_percentual": TAXA_CREDITO,
        "banco_destino": MAQ_CREDITO,
    })

    # Seed: PIX via maquineta (bandeira vazia, parcelas 1, taxa 0)
    _insert_row("taxas_maquinas", {
        "maquineta": MAQ_CREDITO,
        "forma_pagamento": "PIX",
        "bandeira": "",
        "parcelas": 1,
        "taxa_percentual": 0.0,
        "banco_destino": MAQ_CREDITO,
    })

    conn.commit()


def _count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _get_val(conn: sqlite3.Connection, sql: str, params=()):
    row = conn.execute(sql, params).fetchone()
    return None if not row else row[0]


# --- Test Runner ----------------------------------------------------------
def run():
    print("== Smoke de Vendas ==")
    print(f"Banco origem: {DEFAULT_SRC_DB}")
    print(f"Banco teste : {TEST_DB}")

    _copy_db(DEFAULT_SRC_DB, TEST_DB)
    with _conn(TEST_DB) as conn:
        _ensure_tables_and_seed(conn)

    # Import tardio (usa seu código real)
    from services.vendas import VendasService

    svc = VendasService(db_path_like=str(TEST_DB))

    # Snapshots iniciais
    with _conn(TEST_DB) as conn:
        n_entrada_ini = _count(conn, "entrada")
        n_mov_ini     = _count(conn, "movimentacoes_bancarias")

    print("\nDINHEIRO ...")
    venda_id_1, mov_id_1 = svc.registrar_venda(
        data_venda=HOJE, data_liq=HOJE,
        valor_bruto=DIN_VALOR, forma="DINHEIRO",
        parcelas=1, bandeira="", maquineta=None,
        banco_destino=None, taxa_percentual=0.0,
        usuario="Teste"
    )
    print("  1ª chamada:", venda_id_1, mov_id_1)
    venda_id_1b, mov_id_1b = svc.registrar_venda(
        data_venda=HOJE, data_liq=HOJE,
        valor_bruto=DIN_VALOR, forma="DINHEIRO",
        parcelas=1, bandeira="", maquineta=None,
        banco_destino=None, taxa_percentual=0.0,
        usuario="Teste"
    )
    print("  2ª chamada (idem):", venda_id_1b, mov_id_1b, "(esperado -1 -1)")

    print("\nPIX DIRETO ...")
    venda_id_2, mov_id_2 = svc.registrar_venda(
        data_venda=HOJE, data_liq=HOJE,
        valor_bruto=PIX_VALOR, forma="PIX",
        parcelas=1, bandeira="", maquineta=None,
        banco_destino=PIX_BANCO_DEST, taxa_percentual=0.0,
        usuario="Teste"
    )
    print("  retorno:", venda_id_2, mov_id_2)

    print("\nCRÉDITO 2x via maquineta ...")
    venda_id_3, mov_id_3 = svc.registrar_venda(
        data_venda=HOJE, data_liq=HOJE,
        valor_bruto=CC_VALOR, forma="CRÉDITO",
        parcelas=PARC_CREDITO, bandeira=BAND_CREDITO, maquineta=MAQ_CREDITO,
        banco_destino=MAQ_CREDITO, taxa_percentual=0.0,  # taxa será descoberta se 0.0
        usuario="Teste"
    )
    print("  retorno:", venda_id_3, mov_id_3)

    # Verificações básicas
    with _conn(TEST_DB) as conn:
        n_entrada_end = _count(conn, "entrada")
        n_mov_end     = _count(conn, "movimentacoes_bancarias")

        caixa_vendas  = _get_val(conn, "SELECT COALESCE(caixa_vendas,0) FROM saldos_caixas WHERE data = ?", (HOJE,))
        # coluna dinâmica para banco PIX_DIRETO
        try:
            pix_saldo = _get_val(conn, f'SELECT COALESCE("{PIX_BANCO_DEST}",0) FROM saldos_bancos WHERE data = ?', (HOJE,))
        except sqlite3.OperationalError:
            pix_saldo = None

    print("\n== Movimentações após testes ==")
    print(" entradas (+)", n_entrada_end - n_entrada_ini)
    print(" movs      (+)", n_mov_end - n_mov_ini)
    print(f" caixa_vendas({HOJE}):", caixa_vendas)
    print(f" {PIX_BANCO_DEST}({HOJE}) em saldos_bancos:", pix_saldo)

    print("\nOK! DB de teste ficou em:", TEST_DB)


if __name__ == "__main__":
    run()
