import streamlit as st
import pandas as pd
import sqlite3
from services.taxas import TaxaMaquinetaManager

# Página de Cadastro de Taxas por Maquineta =========================================================================
def pagina_taxas_maquinas(caminho_banco: str):
    st.subheader("💳 Cadastro de Taxas por Maquineta")
    manager = TaxaMaquinetaManager(caminho_banco)

    col_form, col_lista = st.columns([2, 1])

    with col_form:
        maquinetas_predefinidas = ["Cielo", "InfinitePay", "Inter", "Outro"]
        maquineta_selecionada = st.selectbox("Selecione a Maquineta", maquinetas_predefinidas)

        if maquineta_selecionada == "Outro":
            maquineta = st.text_input("Digite o nome da nova maquineta").strip()
            if not maquineta:
                st.warning("⚠️ Informe o nome da maquineta antes de continuar.")
                return
        else:
            maquineta = maquineta_selecionada

        # === Novo: carregar bancos cadastrados dinamicamente (logo abaixo da maquineta)
        with sqlite3.connect(caminho_banco) as conn:
            df_bancos = pd.read_sql("SELECT nome FROM bancos_cadastrados ORDER BY nome", conn)
            opcoes_bancos = df_bancos["nome"].tolist()

        if not opcoes_bancos:
            st.warning("⚠️ Nenhum banco cadastrado ainda. Cadastre em 'Cadastro de Bancos'.")
            banco_destino = None
        else:
            banco_destino = st.selectbox("Banco que recebe o valor da maquineta", opcoes_bancos)

        forma_pagamento = st.selectbox("Forma de Pagamento", ["Débito", "Crédito", "PIX"], index=1)

        if forma_pagamento == "Débito":
            opcoes_bandeiras = ["Visa", "Master", "Elo"]
            bandeira = st.selectbox("Bandeira", opcoes_bandeiras)
            parcelas = 1

        elif forma_pagamento == "Crédito":
            opcoes_bandeiras = ["Visa", "Master", "Elo", "Amex", "DinersClub"]
            bandeira = st.selectbox("Bandeira", opcoes_bandeiras)
            parcelas = st.selectbox("Parcelas", list(range(1, 13)))

        elif forma_pagamento == "PIX":
            bandeira = ""
            parcelas = 1

        taxa = st.number_input("Taxa (%)", min_value=0.0, step=0.01, format="%.2f")

        if st.button("💾 Salvar Taxa"):
            if not banco_destino:
                st.warning("⚠️ Selecione um banco válido.")
            else:
                manager.salvar_taxa(maquineta, forma_pagamento, bandeira, parcelas, taxa, banco_destino)
                st.success("✅ Taxa cadastrada com sucesso!")
                st.rerun()

    with col_lista:
        st.markdown("### 🧾 Maquinetas Cadastradas")
        try:
            df = manager.carregar_taxas()
            maquinetas_unicas = sorted(df["Maquineta"].unique())
            if maquinetas_unicas:
                for nome in maquinetas_unicas:
                    st.markdown(f"- ✅ **{nome}**")
            else:
                st.info("Nenhuma maquineta cadastrada ainda.")
        except Exception as e:
            st.error(f"Erro ao carregar maquinetas: {e}")

    st.divider()
    st.markdown("### 📋 Taxas Cadastradas")
    df = manager.carregar_taxas()
    if df.empty:
        st.info("Nenhum cadastro encontrado.")
    else:
        df["Taxa (%)"] = df["Taxa (%)"].apply(lambda x: f"{x:.2f}%")
        st.dataframe(df, use_container_width=True, hide_index=True)