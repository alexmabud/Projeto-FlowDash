# shared/debug_trace.py
"""
Ferramentas simples de depuração para handlers/ações de UI.

O objetivo é **ver a stack trace** no console e, quando disponível, exibir
também no Streamlit sem interromper a re-execução automática do app.

Uso rápido
----------
1) Como *decorator* (ideal para funções chamadas por botões):
    from shared.debug_trace import debug_wrap

    @debug_wrap("Erro ao processar pagamento")
    def on_click():
        ...

2) Como *context manager* (para blocos inline em callbacks):
    from shared.debug_trace import debug_wrap_ctx

    def on_click():
        with debug_wrap_ctx("Erro ao gerar relatório"):
            ...

Notas
-----
- Sempre **re-lança** a exceção após logar (comportamento intencional).
- No Streamlit, tenta exibir `st.error(...)` + `st.code(traceback)`.
"""

from __future__ import annotations

import sys
import traceback
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, Generator, TypeVar, Tuple

__all__ = ["debug_wrap", "debug_wrap_ctx"]

R = TypeVar("R")  # Tipo de retorno da função decorada


def _print_exc(title: str) -> None:
    """Imprime o traceback da exceção atual no console e, se possível, no Streamlit.

    Args:
        title: Título curto para contextualizar o erro no log/UX.
    """
    tb = traceback.format_exc()
    # Console (stderr)
    print(f"\n=== DEBUG TRACE: {title} ===\n{tb}", file=sys.stderr, flush=True)

    # Streamlit (se disponível)
    try:
        import streamlit as st  # type: ignore
        st.error(title)
        st.code(tb)
    except Exception:
        # Sem Streamlit ou ambiente sem frontend
        pass


def debug_wrap(title: str = "Erro no handler") -> Callable[[Callable[..., R]], Callable[..., R]]:
    """Decorator de proteção: loga traceback e re-lança a exceção.

    Use acima de callbacks/handlers (ex.: ações de botão).
    O wrapper não altera a assinatura de chamada e preserva metadados da função.

    Args:
        title: Mensagem de contexto para o log/UX.

    Returns:
        Callable decorado com tratamento de exceção.
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
    """Context manager de proteção: loga traceback e re-lança a exceção.

    Útil quando o código do handler está inline e não dá para usar decorator.

    Args:
        title: Mensagem de contexto para o log/UX.

    Yields:
        None
    """
    try:
        yield
    except Exception:
        _print_exc(title)
        raise
