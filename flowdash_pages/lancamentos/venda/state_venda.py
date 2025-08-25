# ===================== State: Venda =====================
"""
Gerencia estado/transientes da página de Venda (session_state e helpers).
"""

from __future__ import annotations
from dataclasses import dataclass

@dataclass
class VendaState:
    """Estado simples para controle do formulário de venda."""
    form_visivel: bool = False
    confirmado: bool = False


def toggle_form() -> None:
    """Alterna visibilidade do formulário e reinicia confirmação."""
    import streamlit as st
    st.session_state.form_venda = not st.session_state.get("form_venda", False)
    if st.session_state.form_venda:
        st.session_state["venda_confirmar"] = False


def form_visivel() -> bool:
    """Retorna se o formulário está visível."""
    import streamlit as st
    return bool(st.session_state.get("form_venda", False))


def invalidate_confirm() -> None:
    """Invalida a confirmação quando campos críticos mudam (usado em on_change)."""
    import streamlit as st
    st.session_state["venda_confirmar"] = False
