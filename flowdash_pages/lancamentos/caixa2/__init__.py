"""
Página: Lançamentos / Caixa 2
=============================

Agrupa a página, estado, formulários e ações referentes ao **Caixa 2**.

Este pacote NÃO contém regra de negócio de domínio fora do que já existia;
apenas organiza o código em módulos menores.

"""

"""Página de Caixa 2 (Lançamentos)"""
from .page_caixa2 import render_caixa2
__all__ = ["render_caixa2"]
