"""
Pacote Ledger (serviços financeiros).

Mantém compatibilidade com:
    from services.ledger import LedgerService

Ao longo da refatoração, a classe `LedgerService` deve residir em
`service_ledger.py`, e este __init__ apenas a reexporta.
"""
from .service_ledger import LedgerService

__all__ = ["LedgerService"]
