import streamlit as st
from .cadastro_classes import BancoRepository

def pagina_cadastro_bancos(caminho_banco: str):
    st.subheader("🏦 Cadastro de Bancos")
    repo = BancoRepository(caminho_banco)

    st.markdown("Cadastre aqui os bancos para vincular aos lançamentos e relatórios.")

    # --- Cadastro (form) ---
    with st.form("form_cadastro_banco"):
        nome_banco = st.text_input(
            "Nome do novo banco (sem acentos ou espaços no fim/início)",
            placeholder="Ex.: Inter, Bradesco, InfinitePay",
        ).strip()
        submitted = st.form_submit_button("📅 Cadastrar Banco")

    if submitted:
        if not nome_banco:
            st.warning("⚠️ Informe o nome do banco.")
        elif "  " in nome_banco:
            st.warning("⚠️ Remova espaços duplicados.")
        else:
            try:
                repo.salvar_novo_banco(nome_banco)
                st.success(f"✅ Banco '{nome_banco}' cadastrado com sucesso!")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao cadastrar banco: {e}")

    st.markdown("---")
    st.markdown("### 📋 Bancos Cadastrados")
    try:
        df_bancos = repo.carregar_bancos()

        if df_bancos is None or df_bancos.empty:
            st.info("Nenhum banco cadastrado ainda.")
            return

        for _, row in df_bancos.iterrows():
            col1, col2 = st.columns([5, 1])
            with col1:
                st.markdown(f"- 🏦 **{row['nome']}**  \n<small>ID: {row['id']}</small>", unsafe_allow_html=True)
            with col2:
                if st.button("🗑️ Excluir", key=f"excluir_{row['id']}"):
                    try:
                        repo.excluir_banco(int(row["id"]))
                        st.success(f"✅ Banco '{row['nome']}' excluído com sucesso!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro ao excluir banco: {e}")
    except Exception as e:
        st.error(f"Erro ao carregar bancos: {e}")

__all__ = ["pagina_cadastro_bancos"]