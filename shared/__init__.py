"""
Pacote Shared
=============

Módulo utilitário que concentra componentes globais usados em todo o sistema.

Submódulos
----------
- db ........ conexão central SQLite (`get_conn`, etc.)
- ids ....... helpers para geração/sanitização de IDs

Observação
----------
Não existe `shared.actions`, portanto esse import foi removido.
"""

from shared.db import get_conn
from shared.ids import sanitize, uid_saida_dinheiro, uid_saida_bancaria, uid_credito_programado, uid_boleto_programado

__all__ = [
    "get_conn",
    "sanitize",
    "uid_saida_dinheiro",
    "uid_saida_bancaria",
    "uid_credito_programado",
    "uid_boleto_programado",
]
