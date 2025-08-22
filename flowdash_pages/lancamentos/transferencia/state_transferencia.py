# ===================== State: Transferência =====================
"""
Gerencia estado/transientes da página de Transferência (session_state e helpers).
"""

from dataclasses import dataclass

@dataclass
class TransferenciaState:
    """Estado simples para controlar a exibição do formulário e confirmação."""
    form_visivel: bool = False
    confirmado: bool = False

def toggle_form():
    """Alterna visibilidade e reinicia confirmação."""
    import streamlit as st
    st.session_state.form_transf_bancos = not st.session_state.get("form_transf_bancos", False)
    if st.session_state.form_transf_bancos:
        st.session_state["transf_bancos_confirmar"] = False

def form_visivel() -> bool:
    """Retorna se o formulário está visível."""
    import streamlit as st
    return bool(st.session_state.get("form_transf_bancos", False))