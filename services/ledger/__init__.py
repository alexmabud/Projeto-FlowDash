"""
Pacote Ledger
=============

Este pacote reúne os mixins internos usados pelo `LedgerService`.
⚠️ Não reexporta mixins individuais para evitar dependências cíclicas.

Uso recomendado (fora deste pacote):
    from services.ledger.service_ledger import LedgerService
"""

# Exponha somente a fachada pública
from .service_ledger import LedgerService

__all__ = ["LedgerService"]
