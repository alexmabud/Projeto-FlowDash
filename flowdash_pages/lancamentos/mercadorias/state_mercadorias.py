# ===================== State: Mercadorias =====================
"""
Gerencia visibilidade e confirmação dos formulários de Mercadorias.
"""
import streamlit as st

def _ensure_keys():
    st.session_state.setdefault("show_merc_compra", False)
    st.session_state.setdefault("show_merc_receb", False)
    st.session_state.setdefault("merc_compra_confirma_out", False)
    st.session_state.setdefault("merc_receb_confirma_out", False)

def toggle_compra():
    _ensure_keys()
    st.session_state.show_merc_compra = not st.session_state.show_merc_compra
    if st.session_state.show_merc_compra:
        st.session_state.merc_compra_confirma_out = False  # reset confirmação

def toggle_receb():
    _ensure_keys()
    st.session_state.show_merc_receb = not st.session_state.show_merc_receb
    if st.session_state.show_merc_receb:
        st.session_state.merc_receb_confirma_out = False  # reset confirmação

def compra_visivel() -> bool:
    _ensure_keys()
    return bool(st.session_state.show_merc_compra)

def receb_visivel() -> bool:
    _ensure_keys()
    return bool(st.session_state.show_merc_receb)
