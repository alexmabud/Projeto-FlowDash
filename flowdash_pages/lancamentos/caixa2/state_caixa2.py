# ===================== State: Caixa 2 =====================
"""
Gerencia estado/transientes da página Caixa 2 (session_state e helpers).
"""

from dataclasses import dataclass

@dataclass
class Caixa2State:
    """Estado simples para controlar a exibição do formulário."""
    form_visivel: bool = False

def toggle_form():
    """Alterna a flag de visibilidade do formulário na session_state."""
    import streamlit as st
    st.session_state.form_caixa2 = not st.session_state.get("form_caixa2", False)

def form_visivel() -> bool:
    """Retorna se o formulário está visível."""
    import streamlit as st
    return bool(st.session_state.get("form_caixa2", False))