# ===================== State: Transferência =====================
"""
Gerencia o estado/transientes da página de Transferência (session_state e helpers).
"""

from __future__ import annotations

import streamlit as st

__all__ = ["toggle_form", "form_visivel", "invalidate_confirm"]

# ---- Session keys ----
_SS_FORM_FLAG = "form_transferencia"
_SS_CONFIRMADA_KEY = "transferencia_confirmada"


def _ensure_keys() -> None:
    """Garante as chaves necessárias no session_state."""
    st.session_state.setdefault(_SS_FORM_FLAG, False)
    st.session_state.setdefault(_SS_CONFIRMADA_KEY, False)


def toggle_form() -> None:
    """Alterna a visibilidade do formulário e reinicia a confirmação."""
    _ensure_keys()
    st.session_state[_SS_FORM_FLAG] = not bool(st.session_state[_SS_FORM_FLAG])
    if st.session_state[_SS_FORM_FLAG]:
        st.session_state[_SS_CONFIRMADA_KEY] = False


def form_visivel() -> bool:
    """Indica se o formulário está visível.

    Returns:
        True se o formulário deve ser exibido; False caso contrário.
    """
    _ensure_keys()
    return bool(st.session_state[_SS_FORM_FLAG])


def invalidate_confirm() -> None:
    """Invalida a confirmação (usar quando campos críticos forem alterados)."""
    _ensure_keys()
    st.session_state[_SS_CONFIRMADA_KEY] = False
