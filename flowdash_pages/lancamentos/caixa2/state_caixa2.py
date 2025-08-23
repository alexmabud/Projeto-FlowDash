# ===================== State: Caixa 2 =====================
"""
Gerencia estado/transientes da página Caixa 2 (session_state e helpers).
"""
from dataclasses import dataclass
import streamlit as st

@dataclass
class Caixa2State:
    """Estado simples para controlar a exibição do formulário."""
    form_visivel: bool = False

def ensure_state():
    """Garante chaves padrão na session_state."""
    if "caixa2_state" not in st.session_state:
        st.session_state.caixa2_state = Caixa2State()
    if "form_caixa2" not in st.session_state:
        st.session_state.form_caixa2 = False

def toggle_form():
    """Alterna a flag de visibilidade do formulário na session_state."""
    ensure_state()
    st.session_state.form_caixa2 = not st.session_state.form_caixa2
    st.session_state.caixa2_state.form_visivel = st.session_state.form_caixa2

def form_visivel() -> bool:
    """Retorna se o formulário está visível."""
    ensure_state()
    return bool(st.session_state.form_caixa2)
