# ===================== State: Caixa 2 =====================
"""
State: Caixa 2

Resumo:
    Gerencia o estado/transientes da página de Caixa 2 usando `st.session_state`,
    mantendo a visibilidade do formulário em uma chave dedicada (sem conflitar com widgets).

Mantém:
    - Chave exclusiva de visibilidade do form.
    - Helpers simples: abrir/fechar/toggle e consulta de visibilidade.

Saída (funções públicas):
    - toggle_form(): alterna a visibilidade do formulário.
    - form_visivel() -> bool: retorna True se o formulário deve ser exibido.
    - close_form(): força o fechamento (útil após sucesso).
"""

from __future__ import annotations

from dataclasses import dataclass
import streamlit as st

__all__ = ["toggle_form", "form_visivel", "close_form"]

# Key exclusiva para visibilidade (NÃO usar a mesma key de widgets)
_VIS_KEY = "caixa2_form_visivel"


@dataclass
class Caixa2State:
    """Estado simples para controlar a exibição do formulário."""
    form_visivel: bool = False  # padrão: iniciar oculto (abre ao clicar no botão)


def _ensure_state() -> None:
    """Garante as chaves base em session_state (idempotente)."""
    if "caixa2_state" not in st.session_state:
        st.session_state.caixa2_state = Caixa2State()
    if _VIS_KEY not in st.session_state:
        st.session_state[_VIS_KEY] = bool(st.session_state.caixa2_state.form_visivel)


def toggle_form() -> None:
    """Alterna a flag de visibilidade do formulário."""
    _ensure_state()
    st.session_state[_VIS_KEY] = not bool(st.session_state[_VIS_KEY])
    # Mantém o espelho no dataclass (opcional, mas útil para inspeção/log)
    st.session_state.caixa2_state.form_visivel = bool(st.session_state[_VIS_KEY])


def form_visivel() -> bool:
    """Indica se o formulário está visível."""
    _ensure_state()
    return bool(st.session_state[_VIS_KEY])


def close_form() -> None:
    """Fecha o formulário (ex.: após uma operação bem-sucedida)."""
    _ensure_state()
    st.session_state[_VIS_KEY] = False
    st.session_state.caixa2_state.form_visivel = False
