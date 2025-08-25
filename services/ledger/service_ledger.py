# services/ledger/service_ledger.py
"""
service_ledger.py — Fachada do Ledger (compatível 1:1).

Resumo:
    Reúne os mixins em `services/ledger/`, preservando a API/comportamento do
    LedgerService monolítico. Os repositórios continuam acessíveis via attrs
    (`mov_repo`, `cap_repo`, `cartoes_repo`) e a conexão é referenciada por
    `db_path` dentro dos mixins.

Depende de:
    - repository.movimentacoes_repository.MovimentacoesRepository
    - repository.contas_a_pagar_mov_repository.ContasAPagarMovRepository
    - repository.cartoes_repository.CartoesRepository
    - Mixins locais em services/ledger/*
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Imports + bootstrap de caminho (robusto em execuções via Streamlit)
# -----------------------------------------------------------------------------
import logging
import os
import sys

# Garante que a raiz do projeto (<raiz>/services/ledger/..) esteja no sys.path
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# -----------------------------------------------------------------------------
# Repositórios (absolutos a partir da raiz do projeto)
# -----------------------------------------------------------------------------
from repository.movimentacoes_repository import MovimentacoesRepository  # noqa: E402
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository  # noqa: E402
from repository.cartoes_repository import CartoesRepository  # noqa: E402

# -----------------------------------------------------------------------------
# Mixins (preferir import relativo; fallback absoluto se necessário)
# -----------------------------------------------------------------------------
try:
    from .service_ledger_infra import _InfraLedgerMixin  # noqa: E402
    from .service_ledger_cap_helpers import _CapStatusLedgerMixin  # noqa: E402
    from .service_ledger_autobaixa import _AutoBaixaLedgerMixin  # noqa: E402
    from .service_ledger_saida import _SaidasLedgerMixin  # noqa: E402
    from .service_ledger_credito import _CreditoLedgerMixin  # noqa: E402
    from .service_ledger_boleto import _BoletoLedgerMixin  # noqa: E402
    from .service_ledger_fatura import _FaturaLedgerMixin  # noqa: E402
    from .service_ledger_emprestimo import _EmprestimoLedgerMixin  # noqa: E402
except ImportError:
    # Fallback absoluto (caso o import relativo não esteja disponível)
    from services.ledger.service_ledger_infra import _InfraLedgerMixin  # type: ignore  # noqa: E402
    from services.ledger.service_ledger_cap_helpers import _CapStatusLedgerMixin  # type: ignore  # noqa: E402
    from services.ledger.service_ledger_autobaixa import _AutoBaixaLedgerMixin  # type: ignore  # noqa: E402
    from services.ledger.service_ledger_saida import _SaidasLedgerMixin  # type: ignore  # noqa: E402
    from services.ledger.service_ledger_credito import _CreditoLedgerMixin  # type: ignore  # noqa: E402
    from services.ledger.service_ledger_boleto import _BoletoLedgerMixin  # type: ignore  # noqa: E402
    from services.ledger.service_ledger_fatura import _FaturaLedgerMixin  # type: ignore  # noqa: E402
    from services.ledger.service_ledger_emprestimo import _EmprestimoLedgerMixin  # type: ignore  # noqa: E402

logger = logging.getLogger(__name__)

__all__ = ["LedgerService"]


class LedgerService(
    _SaidasLedgerMixin,
    _CreditoLedgerMixin,
    _BoletoLedgerMixin,
    _FaturaLedgerMixin,
    _EmprestimoLedgerMixin,
    _AutoBaixaLedgerMixin,
    _CapStatusLedgerMixin,
    _InfraLedgerMixin,  # base/util por último
):
    """
    Serviço central do Ledger (fachada via mixins).

    Observações:
        - A ordem dos mixins foi definida para preservar dependências internas.
        - Métodos homônimos seguem a resolução MRO (da esquerda para a direita).
    """

    # Atributos criados no __init__ (úteis para type checkers)
    db_path: str
    mov_repo: MovimentacoesRepository
    cap_repo: ContasAPagarMovRepository
    cartoes_repo: CartoesRepository

    def __init__(self, db_path: str) -> None:
        """Inicializa repositórios mantendo compatibilidade 1:1."""
        self.db_path = db_path
        self.mov_repo = MovimentacoesRepository(db_path)
        self.cap_repo = ContasAPagarMovRepository(db_path)
        self.cartoes_repo = CartoesRepository(db_path)

        # Mixins normalmente não implementam __init__, mas chamamos por segurança.
        try:
            super().__init__()  # no-op se os mixins não tiverem __init__
        except TypeError:
            # Algum mixin pode não cooperar com super(); ignoramos com segurança.
            pass

        logger.debug("LedgerService inicializado com db_path=%s", db_path)

    def __repr__(self) -> str:  # helper de depuração
        return f"<LedgerService db_path={self.db_path!r}>"
