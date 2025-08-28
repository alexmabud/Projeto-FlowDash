# ===================== State: Mercadorias =====================
"""Estado/visibilidade dos formulários de Mercadorias.

Responsável por:
    - Alternar visibilidade das seções **Compra** e **Recebimento**.
    - Resetar a confirmação do checkbox ao abrir cada seção.
    - Expor helpers simples para consultar visibilidade atual.

Observação:
    Este módulo não renderiza UI; apenas manipula `st.session_state`.
"""

from __future__ import annotations

from typing import Final

import streamlit as st

# --- Session Keys (constantes) ---
KEY_SHOW_COMPRA: Final[str] = "show_merc_compra"
KEY_SHOW_RECEB: Final[str] = "show_merc_receb"
KEY_CONFIRMA_COMPRA: Final[str] = "merc_compra_confirma_out"
KEY_CONFIRMA_RECEB: Final[str] = "merc_receb_confirma_out"

__all__ = [
    "toggle_compra",
    "toggle_receb",
    "compra_visivel",
    "receb_visivel",
]


def _ensure_keys() -> None:
    """Garante a existência das chaves usadas no `session_state`."""
    st.session_state.setdefault(KEY_SHOW_COMPRA, False)
    st.session_state.setdefault(KEY_SHOW_RECEB, False)
    st.session_state.setdefault(KEY_CONFIRMA_COMPRA, False)
    st.session_state.setdefault(KEY_CONFIRMA_RECEB, False)


def toggle_compra() -> None:
    """Alterna a visibilidade da seção **Compra** e reseta confirmação."""
    _ensure_keys()
    st.session_state[KEY_SHOW_COMPRA] = not st.session_state[KEY_SHOW_COMPRA]
    if st.session_state[KEY_SHOW_COMPRA]:
        # Ao abrir a seção, resetar confirmação do formulário.
        st.session_state[KEY_CONFIRMA_COMPRA] = False


def toggle_receb() -> None:
    """Alterna a visibilidade da seção **Recebimento** e reseta confirmação."""
    _ensure_keys()
    st.session_state[KEY_SHOW_RECEB] = not st.session_state[KEY_SHOW_RECEB]
    if st.session_state[KEY_SHOW_RECEB]:
        # Ao abrir a seção, resetar confirmação do formulário.
        st.session_state[KEY_CONFIRMA_RECEB] = False


def compra_visivel() -> bool:
    """Retorna True se a seção **Compra** estiver visível."""
    _ensure_keys()
    return bool(st.session_state[KEY_SHOW_COMPRA])


def receb_visivel() -> bool:
    """Retorna True se a seção **Recebimento** estiver visível."""
    _ensure_keys()
    return bool(st.session_state[KEY_SHOW_RECEB])
