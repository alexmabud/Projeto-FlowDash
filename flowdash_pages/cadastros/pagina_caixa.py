import streamlit as st
import sqlite3
from datetime import date
from typing import List, Tuple, Optional
import pandas as pd
from utils.utils import formatar_valor
from .cadastro_classes import CaixaRepository

# === Página de Cadastro de Caixa =======================================================================
def pagina_caixa(caminho_banco: str):
    st.subheader("💰 Cadastro de Caixa (Loja e Dinheiro Levado pra Casa)")
    repo = CaixaRepository(caminho_banco)

    # Seleção da data
    data_caixa = st.date_input("Data de Referência", value=date.today())
    data_caixa_str = str(data_caixa)

    # Busca saldos existentes
    resultado = repo.buscar_saldo_por_data(data_caixa_str)

    if resultado:
        # ✅ Lida com retorno sendo dict ou tupla
        caixa_atual = resultado.get("caixa", 0) if isinstance(resultado, dict) else resultado[0]
        caixa2_atual = resultado.get("caixa_2", 0) if isinstance(resultado, dict) else resultado[1]

        # ✅ Mensagem mais clara e personalizada
        st.info(
            f"🔄 Valores já cadastrados para `{data_caixa_str}`:\n\n"
            f"- 💵 **Caixa (loja)**: R$ {caixa_atual:.2f}\n"
            f"- 🏠 **Caixa 2 (casa)**: R$ {caixa2_atual:.2f}\n\n"
            f"📌 O valor digitado abaixo será **somado** a esses saldos."
        )

        valor_novo_caixa = st.number_input("Adicionar ao Caixa (dinheiro na loja)", min_value=0.0, step=10.0, format="%.2f")
        valor_novo_caixa_2 = st.number_input("Adicionar ao Caixa 2 (dinheiro que levou pra casa)", min_value=0.0, step=10.0, format="%.2f")

        valor_final_caixa = caixa_atual + valor_novo_caixa
        valor_final_caixa_2 = caixa2_atual + valor_novo_caixa_2
        atualizar = True
    else:
        st.warning("⚠️ Nenhum valor cadastrado para essa data. Informe o valor inicial.")
        valor_final_caixa = st.number_input("Caixa (dinheiro na loja)", min_value=0.0, step=10.0, format="%.2f")
        valor_final_caixa_2 = st.number_input("Caixa 2 (dinheiro que levou pra casa)", min_value=0.0, step=10.0, format="%.2f")
        atualizar = False

    # Botão para salvar
    if st.button("💾 Salvar Valores"):
        try:
            repo.salvar_saldo(data_caixa_str, valor_final_caixa, valor_final_caixa_2, atualizar)
            st.success("✅ Valores salvos com sucesso!")
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar: {e}")

    st.markdown("---")
    st.markdown("### 📋 Últimos Registros")

    # Visualização dos últimos saldos
    try:
        df_caixa = repo.listar_ultimos_saldos()
        if not df_caixa.empty:
            df_caixa["data"] = pd.to_datetime(df_caixa["data"]).dt.strftime("%d/%m/%Y")

            # ✅ Formata todas as colunas monetárias que existirem
            colunas_monetarias = [
                "caixa", "caixa_2", "caixa_venda", "caixa_total", "caixa2_dia", "caixa2_total"
            ]
            for col in colunas_monetarias:
                if col in df_caixa.columns:
                    df_caixa[col] = df_caixa[col].apply(formatar_valor)

            # ✅ Exibe colunas que realmente existem
            colunas_exibir = [col for col in [
                "data", "caixa", "caixa_venda", "caixa_total",
                "caixa_2", "caixa2_dia", "caixa2_total"
            ] if col in df_caixa.columns]

            st.dataframe(df_caixa[colunas_exibir], use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum dado cadastrado ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar: {e}")