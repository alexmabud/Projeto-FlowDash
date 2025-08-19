import streamlit as st
from datetime import date
import pandas as pd
from .shared import carregar_tabela, get_conn, bloco_resumo_dia
from utils.utils import formatar_valor
from .venda import render_venda
from .saida import render_saida
from .caixa2 import render_caixa2
from .deposito import render_deposito
from .transferencia_bancos import render_transferencia_bancaria
from .mercadorias import render_merc_compra, render_merc_recebimento

def pagina_lancamentos(caminho_banco: str):
    # Mensagem de sucesso (flash)
    if "msg_ok" in st.session_state:
        st.success(st.session_state["msg_ok"])
        del st.session_state["msg_ok"]

    # Data do lançamento
    data_lanc = st.date_input("🗓️ Data do Lançamento", value=date.today(), key="data_lanc")
    st.markdown(f"## 🧾 Lançamentos do Dia — **{data_lanc}**")

    # ===== Preparar dados para o RESUMO (um único retângulo) =====
    df_e = carregar_tabela("entrada", caminho_banco)
    df_s = carregar_tabela("saida", caminho_banco)

    total_vendas = (
        df_e[df_e["Data"].dt.date == data_lanc]["Valor"].sum()
        if ("Valor" in df_e.columns and not df_e.empty) else 0.0
    )
    total_saidas = (
        df_s[df_s["Data"].dt.date == data_lanc]["Valor"].sum()
        if ("Valor" in df_s.columns and not df_s.empty) else 0.0
    )

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # Caixa e Caixa 2 (último saldo)
        row = cur.execute("""
            SELECT caixa_total, caixa2_total
              FROM saldos_caixas
          ORDER BY date(data) DESC, rowid DESC
             LIMIT 1
        """).fetchone()
        caixa_total = float(row[0]) if row and row[0] is not None else 0.0
        caixa2_total = float(row[1]) if row and row[1] is not None else 0.0

        # Totais do dia
        depositos_total = cur.execute("""
            SELECT COALESCE(SUM(valor), 0)
              FROM movimentacoes_bancarias
             WHERE date(data)=? AND origem='deposito'
        """, (str(data_lanc),)).fetchone()[0] or 0.0

        transf_caixa2_total = cur.execute("""
            SELECT COALESCE(SUM(valor), 0)
              FROM movimentacoes_bancarias
             WHERE date(data)=?
               AND (origem='transferencia_caixa' OR observacao LIKE '%Caixa 2%')
        """, (str(data_lanc),)).fetchone()[0] or 0.0

        transf_bancos_total = cur.execute("""
            SELECT COALESCE(SUM(valor), 0)
              FROM movimentacoes_bancarias
             WHERE date(data)=? AND origem='transf_bancos'
        """, (str(data_lanc),)).fetchone()[0] or 0.0

        compras_total = cur.execute("""
            SELECT COALESCE(SUM(Valor_Mercadoria), 0)
              FROM mercadorias
             WHERE date(Data)=?
        """, (str(data_lanc),)).fetchone()[0] or 0.0

        receb_total = cur.execute("""
            SELECT COALESCE(SUM(COALESCE(Valor_Recebido, Valor_Mercadoria)), 0)
              FROM mercadorias
             WHERE Recebimento IS NOT NULL
               AND TRIM(Recebimento) <> ''
               AND date(Recebimento)=?
        """, (str(data_lanc),)).fetchone()[0] or 0.0

        # Saldos dos bancos (tenta detectar colunas nome/saldo)
        try:
            df_bancos_raw = pd.read_sql("SELECT * FROM saldos_bancos", conn)
            if not df_bancos_raw.empty:
                nome_col = next((c for c in df_bancos_raw.columns if c.lower() in ("nome","banco","banco_nome","instituicao")), None)
                saldo_col = next((c for c in df_bancos_raw.columns if c.lower() in ("saldo","valor","saldo_atual","valor_atual")), None)
                if nome_col and saldo_col:
                    df_bancos = df_bancos_raw[[nome_col, saldo_col]].copy()
                    df_bancos.columns = ["Banco","Saldo"]
                else:
                    df_bancos = pd.DataFrame(columns=["Banco","Saldo"])
            else:
                df_bancos = pd.DataFrame(columns=["Banco","Saldo"])
        except Exception:
            df_bancos = pd.DataFrame(columns=["Banco","Saldo"])

    # ===== 📆 Resumo do Dia (UM ÚNICO RETÂNGULO PRETO — bloco_resumo_dia) =====
    st.markdown("### 📆 Resumo do Dia")

    cards = []

    # Linha 1 — Vendas | Saídas
    cards += [
        ("Vendas", formatar_valor(total_vendas)),
        ("Saídas", formatar_valor(total_saidas)),
    ]

    # Linha 2 — Caixa | Caixa 2 | Bancos
    cards += [
        ("Caixa",   formatar_valor(caixa_total)),
        ("Caixa 2", formatar_valor(caixa2_total)),
    ]
    if not df_bancos.empty:
        for _, r in df_bancos.iterrows():
            cards.append((str(r["Banco"]), formatar_valor(r["Saldo"] or 0.0)))

    # Linha 3 — Transferências
    cards += [
        ("→ Caixa 2",         formatar_valor(transf_caixa2_total)),
        ("Depósitos",         formatar_valor(depositos_total)),
        ("Transf. Bancos",    formatar_valor(transf_bancos_total)),
    ]

    # Linha 4 — Mercadorias
    cards += [
        ("Compras Merc.",     formatar_valor(compras_total)),
        ("Receb. Merc.",      formatar_valor(receb_total)),
    ]

    # 🔸 Chamada ÚNICA — mantém o retângulo preto e os valores em verde
    bloco_resumo_dia(cards)

    # ===== Ações =====
    st.markdown("### ➕ Ações")

    # Linha 1: Nova Venda | Saída
    a1, a2 = st.columns(2)
    with a1:
        render_venda(caminho_banco, data_lanc)
    with a2:
        render_saida(caminho_banco, data_lanc)

    # Linha 2: Caixa 2 | Depósito | Transferência entre Bancos
    c1, c2, c3 = st.columns(3)
    with c1:
        render_caixa2(caminho_banco, data_lanc)
    with c2:
        render_deposito(caminho_banco, data_lanc)
    with c3:
        render_transferencia_bancaria(caminho_banco, data_lanc)

    st.markdown("---")

    # ===== Mercadorias =====
    st.markdown("### 📦 Mercadorias")
    render_merc_compra(caminho_banco, data_lanc)
    render_merc_recebimento(caminho_banco, data_lanc)