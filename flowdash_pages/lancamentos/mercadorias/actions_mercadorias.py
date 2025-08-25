# flowdash_pages/lancamentos/mercadorias/actions_mercadorias.py
"""
Ações de Mercadorias
--------------------
Somente funções de negócio chamadas pela página:
- salvar_compra
- carregar_compras
- salvar_recebimento

ATENÇÃO: Não importar nada de UI (streamlit, page_mercadorias, state/ui_forms)
para evitar import circular.
"""

from __future__ import annotations
from typing import Any, Dict, List

__all__ = ["salvar_compra", "carregar_compras", "salvar_recebimento"]


def salvar_compra(caminho_banco: str, payload: Dict[str, Any]) -> str:
    """
    TODO: implemente a gravação da compra.
    Stub apenas para liberar os imports.
    """
    # ... implementação real depois ...
    return "Compra registrada com sucesso."


def carregar_compras(caminho_banco: str, incluir_recebidas: bool = False) -> List[Dict[str, Any]]:
    """
    TODO: implemente o carregamento de compras.
    Stub apenas para liberar os imports.
    """
    # ... implementação real depois ...
    return []


def salvar_recebimento(caminho_banco: str, payload: Dict[str, Any]) -> str:
    """
    TODO: implemente a gravação do recebimento.
    Stub apenas para liberar os imports.
    """
    # ... implementação real depois ...
    return "Recebimento registrado com sucesso."
