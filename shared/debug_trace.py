# shared/debug_trace.py
"""
Ferramentas simples de depuração para handlers/ações de UI.

Objetivo
--------
Exibir o traceback no console (stderr) e, quando possível, também no Streamlit.

Uso rápido
----------
1) Como *decorator* (função chamada por um botão, por exemplo):
    from shared.debug_trace import debug_wrap

    @debug_wrap("Erro ao processar pagamento")
    def on_click():
        ...

2) Como *context manager* (bloco inline):
    from shared.debug_trace import debug_wrap_ctx

    def on_click():
        with debug_wrap_ctx("Erro ao gerar relatório"):
            ...

Observações
----------
- Sempre **re-lança** a exceção após logar (comportamento intencional).
- Se houver Streamlit disponível, mostra `st.error(...)` e `st.code(...)`.
"""

from __future__ import annotations

import sys
import traceback
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, Generator, TypeVar

__all__ = ["debug_wrap", "debug_wrap_ctx"]

R = TypeVar("R")  # Tipo de retorno da função decorada


def _print_exc(title: str) -> None:
    """Imprime o traceback atual no stderr e, se possível, no Streamlit."""
    tb = traceback.format_exc()
    # Console (stderr)
    print(f"\n=== DEBUG TRACE: {title} ===\n{tb}", file=sys.stderr, flush=True)
    # Streamlit (se disponível)
    try:
        import streamlit as st  # type: ignore
        st.error(title)
        st.code(tb)
    except Exception:
        # Ambiente sem Streamlit ou sem frontend — ignorar silenciosamente
        pass


def debug_wrap(title: str = "Erro no handler") -> Callable[[Callable[..., R]], Callable[..., R]]:
    """Decorator de proteção: loga traceback e re-lança a exceção.

    Use acima de callbacks/handlers (ex.: ações de botão).
    """
    def deco(fn: Callable[..., R]) -> Callable[..., R]:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> R:
            try:
                return fn(*args, **kwargs)
            except Exception:
                _print_exc(title)
                raise
        return wrapper
    return deco


@contextmanager
def debug_wrap_ctx(title: str = "Erro no handler") -> Generator[None, None, None]:
    """Context manager de proteção: loga traceback e re-lança a exceção."""
    try:
        yield
    except Exception:
        _print_exc(title)
        raise
