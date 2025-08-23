# ===================== State: Depósito =====================
"""
Gerencia estado/transientes da página Depósito (session_state e helpers).
"""
import streamlit as st

def _ensure_keys():
    if "form_deposito" not in st.session_state:
        st.session_state.form_deposito = False
    if "deposito_confirmar" not in st.session_state:
        st.session_state.deposito_confirmar = False

def toggle_form():
    """Alterna visibilidade e reinicia confirmação."""
    _ensure_keys()
    st.session_state.form_deposito = not st.session_state.form_deposito
    if st.session_state.form_deposito:
        st.session_state.deposito_confirmar = False  # reset confirma ao abrir

def form_visivel() -> bool:
    """Retorna se o formulário está visível."""
    _ensure_keys()
    return bool(st.session_state.form_deposito)
