# Normalmente só marcamos como pacote e expomos subpacotes
from . import lancamentos
# Se você tiver outras sub-seções como dre, dashboard, cadastros dentro de flowdash_pages:
# from . import dre, dashboard, cadastros, fechamento, dataframes

__all__ = ["lancamentos"]