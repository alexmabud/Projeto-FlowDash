# ===================== State: Transferência =====================
"""
Gerencia estado/transientes da página Transferência (session_state e helpers).
"""

import streamlit as st

def _ensure_keys():
    st.session_state.setdefault("form_transferencia", False)
    st.session_state.setdefault("transferencia_confirmada", False)

def toggle_form():
    """Alterna visibilidade do formulário e reinicia confirmação."""
    _ensure_keys()
    st.session_state.form_transferencia = not st.session_state.form_transferencia
    if st.session_state.form_transferencia:
        st.session_state.transferencia_confirmada = False

def form_visivel() -> bool:
    """Retorna se o formulário está visível."""
    _ensure_keys()
    return bool(st.session_state.form_transferencia)

def invalidate_confirm():
    """Invalida a confirmação quando campos críticos mudam."""
    _ensure_keys()
    st.session_state.transferencia_confirmada = False
