"""
Página de Fechamento de Caixa
=============================

Renderiza a tela de fechamento, incluindo saldos bancários,
caixa físico e caixa 2. Mantém a lógica original, mas separada
por responsabilidades.
"""
from .fechamento import render_fechamento

__all__ = ["render_fechamento"]
