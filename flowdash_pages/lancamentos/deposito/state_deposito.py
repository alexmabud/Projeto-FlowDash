# ===================== State: Depósito =====================
"""
Gerencia estado/transientes da página Depósito (session_state e helpers).
Segue o mesmo padrão da Transferência.
"""

from __future__ import annotations

import streamlit as st


def _ensure_keys() -> None:
    """
    Garante a existência das chaves necessárias no `st.session_state`.

    Creates:
        - form_deposito (bool): Flag de visibilidade do formulário.
        - deposito_confirmado (bool): Flag de confirmação do resumo.
    """
    st.session_state.setdefault("form_deposito", False)
    st.session_state.setdefault("deposito_confirmado", False)


def toggle_form() -> None:
    """
    Alterna a visibilidade do formulário de Depósito.

    Comportamento:
        - Inverte `form_deposito`.
        - Ao abrir o formulário (True), reseta `deposito_confirmado` para False.
    """
    _ensure_keys()
    st.session_state.form_deposito = not st.session_state.form_deposito
    if st.session_state.form_deposito:
        st.session_state.deposito_confirmado = False


def form_visivel() -> bool:
    """
    Informa se o formulário de Depósito está visível.

    Returns:
        bool: True se o formulário estiver visível, False caso contrário.
    """
    _ensure_keys()
    return bool(st.session_state.form_deposito)


def invalidate_confirm() -> None:
    """
    Invalida a confirmação dos dados do depósito.

    Uso:
        Chamar no `on_change` de campos críticos do form, para forçar o usuário
        a reconfirmar antes de salvar.
    """
    _ensure_keys()
    st.session_state.deposito_confirmado = False
