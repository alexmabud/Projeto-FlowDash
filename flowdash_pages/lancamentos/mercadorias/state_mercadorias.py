# ===================== State: Mercadorias =====================
"""
Gerencia visibilidade e confirmação dos formulários de Mercadorias.
"""

from dataclasses import dataclass

@dataclass
class MercadoriasState:
    show_compra: bool = False
    show_receb: bool = False
    confirma_compra: bool = False
    confirma_receb: bool = False

def toggle_compra():
    import streamlit as st
    st.session_state["show_merc_compra"] = not st.session_state.get("show_merc_compra", False)
    if st.session_state["show_merc_compra"]:
        st.session_state["merc_compra_confirma_out"] = False

def toggle_receb():
    import streamlit as st
    st.session_state["show_merc_receb"] = not st.session_state.get("show_merc_receb", False)
    if st.session_state["show_merc_receb"]:
        st.session_state["merc_receb_confirma_out"] = False

def compra_visivel() -> bool:
    import streamlit as st
    return bool(st.session_state.get("show_merc_compra", False))

def receb_visivel() -> bool:
    import streamlit as st
    return bool(st.session_state.get("show_merc_receb", False))
