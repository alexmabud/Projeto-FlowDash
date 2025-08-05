import streamlit as st
import pandas as pd
import sqlite3
from flowdash_pages.cadastros.cadastro_classes import CartaoCredito


# Página de Cadastro de Cartões de Crédito ========================================================================
def pagina_cartoes_credito(caminho_banco: str):
    st.subheader("📇 Cadastro de Cartões de Crédito")

    if "mensagem_cartao" not in st.session_state:
        st.session_state.mensagem_cartao = ""
        st.session_state.mostrar_mensagem_cartao = False
    elif not st.session_state.get("mensagem_recente", False):
        st.session_state.mensagem_cartao = ""
        st.session_state.mostrar_mensagem_cartao = False

    # === Formulário de cadastro ===
    with st.form("form_cadastrar_cartao_credito", clear_on_submit=True):
        nome = st.text_input("Nome do Cartão (Ex: Nubank, Inter, Bradesco)")
        col1, col2 = st.columns(2)
        with col1:
            fechamento = st.number_input("Dia do Fechamento da Fatura", 1, 31, step=1)
        with col2:
            vencimento = st.number_input("Dia do Vencimento da Fatura", 1, 31, step=1)

        submitted = st.form_submit_button("💾 Salvar Cartão")

        if submitted:
            if not nome.strip():
                st.warning("⚠️ Informe o nome do cartão.")
            else:
                try:
                    cartao = CartaoCredito(nome, fechamento, vencimento)
                    cartao.salvar(caminho_banco)
                    st.session_state.mensagem_cartao = (
                        f"✅ Cartão **{nome.upper()}** cadastrado com sucesso! "
                        f"💳 Fechamento: dia {fechamento}, Vencimento: dia {vencimento}."
                    )
                    st.session_state.mostrar_mensagem_cartao = True
                    st.session_state.mensagem_recente = True 
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao salvar cartão: {e}")

    if st.session_state.get("mostrar_mensagem_cartao"):
        st.success(st.session_state.mensagem_cartao)
        st.session_state.mensagem_recente = False

    st.markdown("### 📋 Cartões de Crédito Cadastrados")
    try:
        with sqlite3.connect(caminho_banco) as conn:
            df = pd.read_sql("""
                SELECT nome AS Cartão, 
                       fechamento AS 'Fechamento (dia)', 
                       vencimento AS 'Vencimento (dia)' 
                FROM cartoes_credito
                ORDER BY nome
            """, conn)

        if df.empty:
            st.info("ℹ️ Nenhum cartão cadastrado ainda.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Erro ao carregar cartões: {e}")