# ===================== State: Página de Lançamentos =====================
"""Estado básico da página agregadora de Lançamentos (sem classes).

Centraliza as chaves do `st.session_state` e expõe helpers para ler/escrever
o estado global da página.

Chaves:
    - KEY_ABA   : nome da aba ativa (ex.: "resumo").
    - KEY_PEND  : flag para “mostrar pendentes”.
    - KEY_DATA  : filtro de data (string/objeto, a seu critério).

Uso:
    from .state_lancamentos import get_aba, set_aba, ensure
    ensure()
    atual = get_aba()
    set_aba("resumo")
"""

from __future__ import annotations

from typing import Final
import streamlit as st

# --- Session keys ---
KEY_ABA: Final[str] = "lanc_aba"
KEY_PEND: Final[str] = "lanc_mostrar_pendentes"
KEY_DATA: Final[str] = "lanc_filtro_data"


def ensure() -> None:
    """Garante a existência das chaves no `st.session_state`.

    Inicializa com:
        KEY_ABA  -> "resumo"
        KEY_PEND -> False
        KEY_DATA -> None
    """
    st.session_state.setdefault(KEY_ABA, "resumo")
    st.session_state.setdefault(KEY_PEND, False)
    st.session_state.setdefault(KEY_DATA, None)


def get_aba() -> str:
    """Retorna o nome da aba ativa da página de Lançamentos.

    Returns:
        str: Nome da aba ativa (por padrão, "resumo").
    """
    ensure()
    return st.session_state[KEY_ABA]


def set_aba(v: str) -> None:
    """Define a aba ativa da página de Lançamentos.

    Args:
        v: Nome da aba a ser ativada (ex.: "resumo", "detalhes").
    """
    ensure()
    st.session_state[KEY_ABA] = v
