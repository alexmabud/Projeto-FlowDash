import sqlite3
import pandas as pd
from datetime import date, datetime, timedelta
import streamlit as st

from utils.utils import formatar_valor  # já existe no seu projeto

def get_conn(db_path: str):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def carregar_tabela(nome_tabela: str, caminho_banco: str) -> pd.DataFrame:
    try:
        with get_conn(caminho_banco) as conn:
            df = pd.read_sql(f"SELECT * FROM {nome_tabela}", conn)
        if "Data" in df.columns:
            df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()

def bloco_resumo_dia(itens):
    st.markdown(f"""
    <div style='border:1px solid #444;border-radius:10px;padding:20px;background:#1c1c1c;margin:10px 0 20px;'>
        <h4 style='color:#fff;margin:0 0 10px;'>📆 Resumo Financeiro de Hoje</h4>
        <table style='width:100%;margin-top:10px;'>
            <tr>
                {''.join([
                    f"<td style='text-align:center;width:33%;'><div style='color:#ccc;font-weight:600'>{k}</div>"
                    f"<div style='font-size:1.4rem;color:#00FFAA'>{v}</div></td>"
                    for k, v in itens
                ])}
            </tr>
        </table>
    </div>""", unsafe_allow_html=True)

# ====== Regras usadas em venda ======
DIAS_COMPENSACAO = {"DINHEIRO":0, "PIX":0, "DÉBITO":1, "CRÉDITO":1, "LINK_PAGAMENTO":1}

def proximo_dia_util_br(data_base: date, dias: int) -> date:
    try:
        from workalendar.america import BrazilDistritoFederal
        cal = BrazilDistritoFederal()
        d, add = data_base, 0
        while add < dias:
            d += timedelta(days=1)
            if cal.is_working_day(d):
                add += 1
        return d
    except Exception:
        d, add = data_base, 0
        while add < dias:
            d += timedelta(days=1)
            if d.weekday() < 5:
                add += 1
        return d

def inserir_mov_liquidacao_venda(caminho_banco: str, data_: str, banco: str, valor_liquido: float,
                                 observacao: str, referencia_id: int | None):
    if not valor_liquido or valor_liquido <= 0:
        return
    with get_conn(caminho_banco) as conn:
        conn.execute("""
            INSERT INTO movimentacoes_bancarias (data, banco, tipo, valor, origem, observacao, referencia_id)
            VALUES (?, ?, 'entrada', ?, 'vendas', ?, ?)
        """, (data_, banco, float(valor_liquido), observacao, referencia_id))
        conn.commit()

def registrar_caixa_vendas(caminho_banco: str, data_: str, valor: float):
    if not valor or valor <= 0:
        return
    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE saldos_caixas SET caixa_vendas = COALESCE(caixa_vendas,0)+? WHERE data=?",
                (float(valor), data_)
            )
            if cur.rowcount == 0:
                cur.execute("INSERT INTO saldos_caixas (data, caixa_vendas) VALUES (?, ?)", (data_, float(valor)))
        except sqlite3.OperationalError:
            cur.execute(
                "UPDATE saldos_caixas SET caixa_vendas = COALESCE(caixa_vendas,0)+? WHERE Data=?",
                (float(valor), data_)
            )
            if cur.rowcount == 0:
                cur.execute("INSERT INTO saldos_caixas (Data, caixa_vendas) VALUES (?, ?)", (data_, float(valor)))
        conn.commit()

def obter_banco_destino(caminho_banco: str, forma: str, maquineta: str, bandeira: str | None, parcelas: int | None) -> str | None:
    formas_try = [forma]
    if forma == "LINK_PAGAMENTO":
        formas_try.append("CRÉDITO")
    with get_conn(caminho_banco) as conn:
        for f in formas_try:
            row = conn.execute("""
                SELECT banco_destino FROM taxas_maquinas
                WHERE forma_pagamento=? AND maquineta=? AND bandeira=? AND parcelas=?
                LIMIT 1
            """, (f, maquineta or "", bandeira or "", int(parcelas or 1))).fetchone()
            if row and row[0]:
                return row[0]
        for f in formas_try:
            row = conn.execute("""
                SELECT banco_destino FROM taxas_maquinas
                WHERE forma_pagamento=? AND maquineta=? AND banco_destino IS NOT NULL AND TRIM(banco_destino)<>''
                LIMIT 1
            """, (f, maquineta or "")).fetchone()
            if row and row[0]:
                return row[0]
        row = conn.execute("""
            SELECT banco_destino FROM taxas_maquinas
            WHERE maquineta=? AND banco_destino IS NOT NULL AND TRIM(banco_destino)<>''
            LIMIT 1
        """, (maquineta or "",)).fetchone()
        if row and row[0]:
            return row[0]
    return None