import streamlit as st
from datetime import date
import pandas as pd
import sqlite3
from flowdash_pages.cadastros.cadastro_classes import SaldoBancarioRepository

# === Página de Cadastro de Saldos Bancários ============================================================
def pagina_saldos_bancarios(caminho_banco: str):
    st.subheader("🏦 Cadastro de Saldos Bancários por Data")
    repo = SaldoBancarioRepository(caminho_banco)

    data_saldo = st.date_input("Data do Saldo", value=date.today())
    data_str = str(data_saldo)

    saldo_existente = repo.obter_saldo_por_data(data_str)

    if saldo_existente:
        inter = st.number_input("Saldo Banco Inter", value=saldo_existente[0], step=10.0, format="%.2f")
        infinitepay = st.number_input("Saldo InfinitePay", value=saldo_existente[1], step=10.0, format="%.2f")
        bradesco = st.number_input("Saldo Bradesco", value=saldo_existente[2], step=10.0, format="%.2f")
        outros = st.number_input("Saldo Outros Bancos", value=saldo_existente[3], step=10.0, format="%.2f")

        st.info(
            f"🔄 Valores já cadastrados para `{data_str}`:\n\n"
            f"- 🏦 **Banco Inter**: R$ {inter:.2f}\n"
            f"- 💳 **InfinitePay**: R$ {infinitepay:.2f}\n"
            f"- 🏛️ **Bradesco**: R$ {bradesco:.2f}\n"
            f"- 🏦 **Outros Bancos**: R$ {outros:.2f}\n\n"
            f"📌 O valor digitado acima **substituirá** o saldo atual dessa data."
        )
    else:
        st.warning("⚠️ Nenhum valor cadastrado para essa data.")
        inter = st.number_input("Saldo Banco Inter", step=10.0, format="%.2f")
        infinitepay = st.number_input("Saldo InfinitePay", step=10.0, format="%.2f")
        bradesco = st.number_input("Saldo Bradesco", step=10.0, format="%.2f")
        outros = st.number_input("Saldo Outros Bancos", step=10.0, format="%.2f")

    if st.button("💾 Salvar Saldos"):
        try:
            repo.salvar_saldo(data_str, inter, infinitepay, bradesco, outros)
            st.success("✅ Saldos salvos com sucesso!")
            st.rerun()
        except Exception as e:
            st.error(f"❌ Erro ao salvar: {e}")

    # Tabela de saldos anteriores
    st.markdown("---")
    st.markdown("### 📋 Saldos Bancários Cadastrados Anteriores")

    try:
        with sqlite3.connect(caminho_banco) as conn:
            df_saldos = pd.read_sql("SELECT * FROM saldos_bancos ORDER BY data DESC LIMIT 15", conn)

        if not df_saldos.empty:
            df_saldos = df_saldos.rename(columns={
                "data": "Data",
                "banco_1": "Banco Inter",
                "banco_2": "InfinitePay",
                "banco_3": "Bradesco",
                "banco_4": "Outros Bancos"
            })
            df_saldos["Data"] = pd.to_datetime(df_saldos["Data"]).dt.strftime("%d/%m/%Y")
            for col in ["Banco Inter", "InfinitePay", "Bradesco", "Outros Bancos"]:
                df_saldos[col] = df_saldos[col].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

            st.dataframe(df_saldos, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum saldo registrado ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar saldos: {e}")