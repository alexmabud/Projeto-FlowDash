# ===================== State: Venda =====================
"""
Gerencia estado/transientes da página de Venda (session_state e helpers).
"""

from __future__ import annotations
import streamlit as st


def toggle_form() -> None:
    """Alterna visibilidade do formulário e reinicia confirmação."""
    st.session_state.form_venda = not st.session_state.get("form_venda", False)
    if st.session_state.form_venda:
        st.session_state["venda_confirmar"] = False


def form_visivel() -> bool:
    """Retorna se o formulário está visível."""
    return bool(st.session_state.get("form_venda", False))


def invalidate_confirm() -> None:
    """Invalida a confirmação quando campos críticos mudam (usado em on_change)."""
    st.session_state["venda_confirmar"] = False
