# shared/debug_trace.py
from __future__ import annotations
import traceback
from contextlib import contextmanager

def _print_exc(title: str):
    tb = traceback.format_exc()
    # Console
    print("\n=== DEBUG TRACE:", title, "===\n", tb)
    # Se for Streamlit, mostra na tela também
    try:
        import streamlit as st
        st.error(title)
        st.code(tb)
    except Exception:
        pass

def debug_wrap(title: str = "Erro no handler"):
    """Decorator: use acima da função que roda no clique do botão."""
    def deco(fn):
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception:
                _print_exc(title)
                raise
        return wrapper
    return deco

@contextmanager
def debug_wrap_ctx(title: str = "Erro no handler"):
    """Context manager: use com 'with' se o código do botão for inline."""
    try:
        yield
    except Exception:
        _print_exc(title)
        raise
