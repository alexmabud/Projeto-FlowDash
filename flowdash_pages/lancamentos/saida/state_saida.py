# ===================== State: Saída =====================
"""
Gerencia estado/transientes da página Saída (session_state e helpers).
"""

from dataclasses import dataclass

@dataclass
class SaidaState:
    """Estado da página de saída."""
    form_visivel: bool = False
    confirmado: bool = False

def toggle_form():
    """Alterna visibilidade do formulário de saída e reinicia confirmação."""
    import streamlit as st
    st.session_state.form_saida = not st.session_state.get("form_saida", False)
    if st.session_state.form_saida:
        st.session_state["confirmar_saida"] = False

def form_visivel() -> bool:
    """Retorna se o formulário está visível."""
    import streamlit as st
    return bool(st.session_state.get("form_saida", False))

def invalidate_confirm():
    """Invalida a confirmação quando campos críticos mudam."""
    import streamlit as st
    st.session_state["confirmar_saida"] = False