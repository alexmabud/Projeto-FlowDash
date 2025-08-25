"""
Módulo Contas a Pagar (Repositório - Facade)
============================================

Expõe a classe `ContasAPagarMovRepository` mantendo a API pública original,
delegando a lógica real para mixins organizados por responsabilidade.

Estrutura
---------
- BaseRepo .......... conexão/PRAGMAs e utilitários internos (vem por ÚLTIMO no MRO).
- EventsMixin ....... LANCAMENTO / PAGAMENTO / AJUSTE.
- PaymentsMixin ..... validações e pagamentos de parcela (boletos).
- AdjustmentsMixin .. MULTA / JUROS / DESCONTO (boletos).
- QueriesMixin ...... consultas para UI (em aberto, saldos, detalhamento).
- LoansMixin ........ geração de parcelas de empréstimos.

Regra de MRO
------------
Mixins **não** herdam de BaseRepo nem entre si. Na classe final, `BaseRepo`
deve vir **por último** na lista de bases.
"""

from repository.contas_a_pagar_mov_repository.types import TipoObrigacao  # re-export
from repository.contas_a_pagar_mov_repository.base import BaseRepo
from repository.contas_a_pagar_mov_repository.events import EventsMixin
from repository.contas_a_pagar_mov_repository.payments import PaymentsMixin
from repository.contas_a_pagar_mov_repository.adjustments import AdjustmentsMixin
from repository.contas_a_pagar_mov_repository.queries import QueriesMixin
from repository.contas_a_pagar_mov_repository.loans import LoansMixin


class ContasAPagarMovRepository(
    EventsMixin,
    PaymentsMixin,
    AdjustmentsMixin,
    QueriesMixin,
    LoansMixin,
    BaseRepo,  # <<< Base concreta sempre por último
):
    """Facade que agrega as funcionalidades do repositório de contas a pagar."""
    def __init__(self, db_path: str, *args, **kwargs):
        # __init__ cooperativo para múltipla herança
        super().__init__(db_path, *args, **kwargs)


# API pública explícita
__all__ = ["ContasAPagarMovRepository", "TipoObrigacao"]
