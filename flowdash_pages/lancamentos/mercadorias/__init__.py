"""
Página: Lançamentos / Mercadorias
=================================

Organiza página, estado, formulários e ações de **Mercadorias**:
- Compra de mercadorias (cadastro + previsões)
- Recebimento (efetivo + divergências)

"""
"""Página de Mercadorias (Lançamentos)"""
from .page_mercadorias import render_mercadorias
__all__ = ["render_mercadorias"]
