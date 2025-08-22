"""
Página agregadora (Lançamentos)
===============================

Orquestra o resumo do dia e carrega as subpáginas de lançamentos.
Mantém a mesma lógica do módulo original, apenas separada por responsabilidades.
"""
from .page_lancamentos import render_page
__all__ = ["render_page"]