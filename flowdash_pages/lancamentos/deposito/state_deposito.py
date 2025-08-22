# ===================== State: Depósito =====================
"""
Gerencia estado/transientes da página Depósito (session_state e helpers).
"""

from dataclasses import dataclass

@dataclass
class DepositoState:
    """Estado simples para controlar a exibição do formulário e confirmação."""
    form_visivel: bool = False
    confirmado: bool = False

def toggle_form():
    """Alterna visibilidade e reinicia confirmação."""
    import streamlit as st
    st.session_state.form_deposito = not st.session_state.get("form_deposito", False)
    if st.session_state.form_deposito:
        st.session_state["deposito_confirmar"] = False

def form_visivel() -> bool:
    """Retorna se o formulário está visível."""
    import streamlit as st
    return bool(st.session_state.get("form_deposito", False))