# ===================== State: Venda =====================
"""
Gerencia estado/transientes da página de Venda (session_state e helpers).
"""

from dataclasses import dataclass

@dataclass
class VendaState:
    """Estado simples para controle do formulário de venda."""
    form_visivel: bool = False
    confirmado: bool = False

def toggle_form():
    """Alterna visibilidade do formulário e reinicia confirmação."""
    import streamlit as st
    st.session_state.form_venda = not st.session_state.get("form_venda", False)
    if st.session_state.form_venda:
        st.session_state["venda_confirmar"] = False

def form_visivel() -> bool:
    """Retorna se o formulário está visível."""
    import streamlit as st
    return bool(st.session_state.get("form_venda", False))