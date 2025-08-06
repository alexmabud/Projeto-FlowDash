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

    # Formulário de correção manual
    data_corrigir = st.date_input("Data do Ajuste", value=date.today())
    valor_ajuste = st.number_input("Valor de Correção (positivo ou negativo)", step=10.0, format="%.2f")
    observacao = st.text_input("Motivo ou Observação", max_chars=200)

    if st.button("💾 Salvar Ajuste Manual"):
        try:
            repo.salvar_ajuste(str(data_corrigir), valor_ajuste, observacao)
            st.success("✅ Ajuste salvo com sucesso!")
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar correção: {e}")

    # Verifica se já existe correção salva para a data selecionada
    try:
        df_ajustes = repo.listar_ajustes()
        if not df_ajustes.empty:
            df_ajustes["data"] = pd.to_datetime(df_ajustes["data"], errors="coerce")
            ajustes_data = df_ajustes[df_ajustes["data"].dt.date == data_corrigir]

            if not ajustes_data.empty:
                ultimo = ajustes_data.iloc[-1]
                valor_formatado = f"R$ {ultimo['valor']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                obs = ultimo["observacao"] if ultimo["observacao"] else "Nenhuma"
                st.success(
                    f"✅ Correção registrada para o dia **{data_corrigir.strftime('%d/%m/%Y')}**:\n\n"
                    f"- 💰 Valor: {valor_formatado}\n"
                    f"- 📝 Observação: {obs}"
                )
    except Exception as e:
        st.error(f"Erro ao verificar correções do dia: {e}")

    # Tabela de ajustes anteriores
    try:
        df_ajustes = repo.listar_ajustes()
        if not df_ajustes.empty:
            df_ajustes["data"] = pd.to_datetime(df_ajustes["data"]).dt.strftime("%d/%m/%Y")
            df_ajustes["valor"] = df_ajustes["valor"].apply(formatar_valor)

            df_ajustes.rename(columns={
                "data": "Data",
                "valor": "Valor (R$)",
                "observacao": "Observação"
            }, inplace=True)

            st.dataframe(df_ajustes[["Data", "Valor (R$)", "Observação"]], use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum ajuste registrado ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar ajustes: {e}")