"""
Página de Fechamento de Caixa
=============================

Renderiza a tela de fechamento, incluindo saldos bancários,
caixa físico e caixa 2. Mantém a lógica original, mas separada
por responsabilidades.
"""
from .fechamento import pagina_fechamento_caixa

__all__ = ["pagina_fechamento_caixa"]
