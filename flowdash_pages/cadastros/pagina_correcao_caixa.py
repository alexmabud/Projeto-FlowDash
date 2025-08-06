import streamlit as st
import sqlite3
import pandas as pd
from datetime import date
from utils.utils import formatar_valor
from .cadastro_classes import CorrecaoCaixaRepository

# Página de Correção Manual de Caixa =================================================================================
def pagina_correcao_caixa(caminho_banco: str):
    st.subheader("🛠️ Correção Manual de Caixa")
    repo = CorrecaoCaixaRepository(caminho_banco)

    # Inicializa estado da mensagem de sucesso
    if "correcao_sucesso" not in st.session_state:
        st.session_state["correcao_sucesso"] = False

    # Formulário de correção manual
    data_corrigir = st.date_input("Data do Ajuste", value=date.today())
    valor_ajuste = st.number_input("Valor de Correção (positivo ou negativo)", step=10.0, format="%.2f")
    observacao = st.text_input("Motivo ou Observação", max_chars=200)

    if st.button("💾 Salvar Ajuste Manual"):
        try:
            repo.salvar_ajuste(str(data_corrigir), valor_ajuste, observacao)
            st.session_state["correcao_sucesso"] = True
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar correção: {e}")

    # Exibe mensagem de sucesso uma vez
    if st.session_state.get("correcao_sucesso"):
        st.success("✅ Ajuste salvo com sucesso!")
        st.session_state["correcao_sucesso"] = False

    # Tabela de ajustes
    st.markdown("### 📋 Ajustes Registrados")
    try:
        df_ajustes = repo.listar_ajustes()
        if not df_ajustes.empty:
            df_ajustes["valor"] = df_ajustes["valor"].apply(formatar_valor)
            df_ajustes["data"] = pd.to_datetime(df_ajustes["data"]).dt.strftime("%d/%m/%Y")

            df_ajustes.rename(columns={
                "data": "Data",
                "valor": "Valor (R$)",
                "observacao": "Observação"
            }, inplace=True)

            st.dataframe(df_ajustes[["Data", "Valor (R$)", "Observação"]], use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum ajuste registrado ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar correções: {e}")