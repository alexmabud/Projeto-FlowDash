"""
Módulo Contas a Pagar (Repositório - Facade)
============================================

Expõe a classe `ContasAPagarMovRepository` mantendo a API pública original,
delegando a lógica real para mixins organizados por responsabilidade.

- BaseRepo: conexão/PRAGMAs e utilitários internos.
- EventsMixin: LANCAMENTO / PAGAMENTO / AJUSTE.
- PaymentsMixin: validações e pagamentos de parcela (boletos).
- AdjustmentsMixin: MULTA / JUROS / DESCONTO (boletos).
- QueriesMixin: consultas para UI (em aberto, saldos, detalhamento).
- LoansMixin: geração de parcelas de empréstimos.
"""

from .types import TipoObrigacao  # re-export  # noqa: F401
from .base import BaseRepo
from .events import EventsMixin
from .payments import PaymentsMixin
from .adjustments import AdjustmentsMixin
from .queries import QueriesMixin
from .loans import LoansMixin


class ContasAPagarMovRepository(
    BaseRepo, EventsMixin, PaymentsMixin, AdjustmentsMixin, QueriesMixin, LoansMixin
):
    """Facade que agrega as funcionalidades do repositório de contas a pagar."""
    def __init__(self, db_path: str):
        super().__init__(db_path)


__all__ = ["ContasAPagarMovRepository", "TipoObrigacao"]
