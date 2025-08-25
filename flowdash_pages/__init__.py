"""
Pacote Flowdash Pages
=====================

Contém as páginas principais do sistema FlowDash (baseadas em Streamlit).

Subpacotes
----------
- cadastros ....... telas de cadastro (categorias, usuários, etc.)
- dashboard ....... visualizações principais do sistema
- dataframes ...... utilitários de DataFrame para visualização
- dre ............. módulo de Demonstração de Resultado (DRE)
- fechamento ...... módulo de fechamento de caixa
- lancamentos ..... páginas de lançamentos (entrada, saída, transferência, etc.)
- metas ........... cadastro e acompanhamento de metas
"""

from . import cadastros, dashboard, dataframes, dre, fechamento, lancamentos, metas

__all__ = [
    "cadastros",
    "dashboard",
    "dataframes",
    "dre",
    "fechamento",
    "lancamentos",
    "metas",
]
