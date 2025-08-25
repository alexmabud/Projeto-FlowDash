from types import SimpleNamespace
from datetime import date
from services.vendas import VendasService
from shared.db import get_conn
import pandas as pd

DB = "data/flowdash_data.db"  # ajuste se necessário
DB_LIKE = SimpleNamespace(caminho_banco=DB)  # testa SimpleNamespace

def ensure_min_schema(db_like):
    with get_conn(db_like) as conn:
        # tabelas mínimas usadas pelo serviço
        conn.execute("""CREATE TABLE IF NOT EXISTS movimentacoes_bancarias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT, banco TEXT, tipo TEXT, valor REAL, origem TEXT,
            observacao TEXT, referencia_tabela TEXT, referencia_id INTEGER, trans_uid TEXT UNIQUE
        );""")
        conn.execute("""CREATE TABLE IF NOT EXISTS saldos_caixas (
            data TEXT PRIMARY KEY, caixa REAL, caixa_2 REAL, caixa_vendas REAL,
            caixa2_dia REAL, caixa_total REAL, caixa2_total REAL
        );""")
        conn.execute("""CREATE TABLE IF NOT EXISTS saldos_bancos (
            data TEXT PRIMARY KEY
        );""")
        conn.execute("""CREATE TABLE IF NOT EXISTS entrada (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Data TEXT, Data_Liq TEXT, Data_Liquidacao TEXT,
            Valor_Bruto REAL, Valor_Liquido REAL, Valor REAL,
            Forma_de_Pagamento TEXT, Forma TEXT, Parcelas INTEGER,
            Bandeira TEXT, Maquineta TEXT, Banco_Destino TEXT,
            Taxa_Percentual REAL, Usuario TEXT, Observacao TEXT
        );""")
        conn.commit()

def dump_movs(db_like, title):
    with get_conn(db_like) as conn:
        df = pd.read_sql("""
            SELECT
                id,
                DATE(data)            AS data,
                banco,
                tipo,
                ROUND(valor, 2)       AS valor,
                origem,
                SUBSTR(trans_uid,1,10) AS uid10
            FROM movimentacoes_bancarias
            ORDER BY id
        """, conn)
    print(f"\n== {title} ==")
    print(df.to_string(index=False))

def test_dinheiro():
    svc = VendasService(DB_LIKE)
    hoje = date.today().isoformat()
    venda_id, mov_id = svc.registrar_venda(
        data_venda=hoje,
        data_liq=hoje,
        valor_bruto=100.00,
        forma="DINHEIRO",
        parcelas=1,
        bandeira="",
        maquineta="",
        banco_destino=None,
        taxa_percentual=0.0,
        usuario="Teste",
    )
    print("DINHEIRO 1ª chamada:", venda_id, mov_id)
    # idempotência
    venda_id2, mov_id2 = svc.registrar_venda(
        data_venda=hoje,
        data_liq=hoje,
        valor_bruto=100.00,
        forma="DINHEIRO",
        parcelas=1,
        bandeira="",
        maquineta="",
        banco_destino=None,
        taxa_percentual=0.0,
        usuario="Teste",
    )
    print("DINHEIRO 2ª chamada (idem):", venda_id2, mov_id2)

def test_pix_direto():
    svc = VendasService(DB_LIKE)
    hoje = date.today().isoformat()
    venda_id, mov_id = svc.registrar_venda(
        data_venda=hoje,
        data_liq=hoje,
        valor_bruto=200.00,
        forma="PIX",
        parcelas=1,
        bandeira="",
        maquineta="",
        banco_destino="Banco_PIX",
        taxa_percentual=1.5,
        usuario="Teste",
    )
    print("PIX direto 1ª chamada:", venda_id, mov_id)
    venda_id2, mov_id2 = svc.registrar_venda(
        data_venda=hoje,
        data_liq=hoje,
        valor_bruto=200.00,
        forma="PIX",
        parcelas=1,
        bandeira="",
        maquineta="",
        banco_destino="Banco_PIX",
        taxa_percentual=1.5,
        usuario="Teste",
    )
    print("PIX direto 2ª chamada (idem):", venda_id2, mov_id2)

def test_credito_2x():
    svc = VendasService(DB_LIKE)
    hoje = date.today().isoformat()
    venda_id, mov_id = svc.registrar_venda(
        data_venda=hoje,
        data_liq=hoje,              # para o smoke test usamos mesma data
        valor_bruto=300.00,
        forma="CRÉDITO",
        parcelas=2,
        bandeira="VISA",
        maquineta="Stone",
        banco_destino="Itau",
        taxa_percentual=3.2,
        usuario="Teste",
    )
    print("CRÉDITO 2x 1ª chamada:", venda_id, mov_id)
    venda_id2, mov_id2 = svc.registrar_venda(
        data_venda=hoje,
        data_liq=hoje,
        valor_bruto=300.00,
        forma="CRÉDITO",
        parcelas=2,
        bandeira="VISA",
        maquineta="Stone",
        banco_destino="Itau",
        taxa_percentual=3.2,
        usuario="Teste",
    )
    print("CRÉDITO 2x 2ª chamada (idem):", venda_id2, mov_id2)

if __name__ == "__main__":
    ensure_min_schema(DB_LIKE)
    test_dinheiro()
    test_pix_direto()
    test_credito_2x()
    dump_movs(DB_LIKE, "Movimentações após testes")
