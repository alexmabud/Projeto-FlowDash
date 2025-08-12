import streamlit as st
from datetime import date
from .shared import carregar_tabela, bloco_resumo_dia
from utils.utils import formatar_valor
from .venda import render_venda
from .saida import render_saida
from .caixa2 import render_caixa2
from .deposito import render_deposito
from .mercadorias import render_mercadorias

def pagina_lancamentos(caminho_banco: str):
    if "msg_ok" in st.session_state:
        st.success(st.session_state["msg_ok"])
        del st.session_state["msg_ok"]

    data_lanc = st.date_input("🗓️ Data do Lançamento", value=date.today(), key="data_lanc")
    st.markdown(f"## 🧾 Lançamentos do Dia — **{data_lanc}**")

    df_e = carregar_tabela("entrada", caminho_banco)
    df_s = carregar_tabela("saida", caminho_banco)
    df_m = carregar_tabela("mercadorias", caminho_banco)

    total_e = df_e[df_e["Data"].dt.date == data_lanc]["Valor"].sum() if "Valor" in df_e.columns else 0.0
    total_s = df_s[df_s["Data"].dt.date == data_lanc]["Valor"].sum() if "Valor" in df_s.columns else 0.0
    total_m = df_m[df_m["Data"].dt.date == data_lanc]["Valor_Mercadoria"].sum() if "Valor_Mercadoria" in df_m.columns else 0.0

    bloco_resumo_dia([
        ("Vendas", formatar_valor(total_e)),
        ("Saídas", formatar_valor(total_s)),
        ("Mercadorias", formatar_valor(total_m)),
    ])

    st.markdown("### ➕ Ações")
    c1, c2 = st.columns(2)

    with c1:
        render_venda(caminho_banco, data_lanc)
        render_caixa2(caminho_banco, data_lanc)

    with c2:
        render_saida(caminho_banco, data_lanc)
        render_deposito(caminho_banco, data_lanc)

    st.markdown("---")
    render_mercadorias(caminho_banco, data_lanc)