import streamlit as st
import sqlite3
import pandas as pd
from .cadastro_classes import BancoRepository


# === Página: Cadastro de Bancos ==========================================================================

def pagina_cadastro_bancos(caminho_banco: str):
    from .cadastro_classes import BancoRepository  # <- Garante que está importando a classe corretamente

    st.subheader("🏦 Cadastro de Bancos")

    repo = BancoRepository(caminho_banco)

    st.markdown(
        "Cadastre aqui os bancos para vincular às maquinetas e gerar colunas automáticas na tabela `saldos_bancos`."
    )

    nome_banco = st.text_input("Nome do novo banco (sem acentos ou espaços)").strip()

    if st.button("📅 Cadastrar Banco"):
        if not nome_banco:
            st.warning("⚠️ Informe o nome do banco.")
        else:
            try:
                repo.salvar_novo_banco(nome_banco)
                st.success(f"✅ Banco '{nome_banco}' cadastrado com sucesso!")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao cadastrar banco: {e}")

    # Listagem com opção de excluir
    st.markdown("---")
    st.markdown("### 📋 Bancos Cadastrados")
    try:
        df_bancos = repo.carregar_bancos()

        if not df_bancos.empty:
            for _, row in df_bancos.iterrows():
                col1, col2 = st.columns([5, 1])
                with col1:
                    st.markdown(f"- 🏦 **{row['nome']}**")
                with col2:
                    if st.button("🗑️", key=f"excluir_{row['id']}"):
                        try:
                            repo.excluir_banco(row['id'])
                            st.success(f"✅ Banco '{row['nome']}' excluído com sucesso!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao excluir banco: {e}")
        else:
            st.info("Nenhum banco cadastrado ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar bancos: {e}")