# ===================== State: Saída =====================
"""
Gerencia estado/transientes da página Saída (session_state e helpers).
"""

import streamlit as st

def _ensure_keys():
    st.session_state.setdefault("form_saida", False)
    st.session_state.setdefault("confirmar_saida", False)

def toggle_form():
    """Alterna visibilidade do formulário de saída e reinicia confirmação."""
    _ensure_keys()
    st.session_state.form_saida = not st.session_state.form_saida
    if st.session_state.form_saida:
        st.session_state.confirmar_saida = False  # reset confirmação ao abrir

def form_visivel() -> bool:
    """Retorna se o formulário está visível."""
    _ensure_keys()
    return bool(st.session_state.form_saida)

def invalidate_confirm():
    """Invalida a confirmação quando campos críticos mudam."""
    _ensure_keys()
    st.session_state.confirmar_saida = False