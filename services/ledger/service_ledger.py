# services/ledger/service_ledger.py
"""service_ledger.py — Fachada do Ledger (compatível 1:1).

Reúne os mixins em `services/ledger/`, mantendo 100% da API e do comportamento
do LedgerService monolítico.
"""
from __future__ import annotations
from repository.movimentacoes_repository import MovimentacoesRepository
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository
from repository.cartoes_repository import CartoesRepository

# ⬇️ imports entre irmãos (estamos dentro de services/ledger)
from .service_ledger_infra import _InfraLedgerMixin
from .service_ledger_cap_helpers import _CapStatusLedgerMixin
from .service_ledger_autobaixa import _AutoBaixaLedgerMixin
from .service_ledger_saida import _SaidasLedgerMixin
from .service_ledger_credito import _CreditoLedgerMixin
from .service_ledger_boleto import _BoletoLedgerMixin
from .service_ledger_fatura import _FaturaLedgerMixin
from .service_ledger_emprestimo import _EmprestimoLedgerMixin


class LedgerService(
    _SaidasLedgerMixin,
    _CreditoLedgerMixin,
    _BoletoLedgerMixin,
    _FaturaLedgerMixin,
    _EmprestimoLedgerMixin,
    _AutoBaixaLedgerMixin,
    _CapStatusLedgerMixin,
    _InfraLedgerMixin,  # base por último
):
    """Serviço central do Ledger (fachada via mixins)."""

    def __init__(self, db_path: str) -> None:
        """Inicializa repositórios mantendo compatibilidade 1:1."""
        self.db_path = db_path
        self.mov_repo = MovimentacoesRepository(db_path)
        self.cap_repo = ContasAPagarMovRepository(db_path)
        self.cartoes_repo = CartoesRepository(db_path)
        try:
            super().__init__()  # no-op se os mixins não tiverem __init__
        except TypeError:
            pass


__all__ = ["LedgerService"]
