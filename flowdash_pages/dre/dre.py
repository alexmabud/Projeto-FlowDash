import streamlit as st

def render_dre(caminho_banco: str):
    """Ponto de entrada padrão da página DRE."""
    st.subheader("📉 DRE")
    st.info("🚧 Em desenvolvimento...")

# Alias para retrocompatibilidade
pagina_dre = render_dre
