# services/ledger/service_ledger.py
"""
Fachada do Ledger — agrega mixins de forma robusta (compat),
com resolução dinâmica para reduzir acoplamento entre refactors.

Principais pontos:
- Repositórios resolvidos via importlib (evita que Pylance aponte import estático quebrado).
- Mixins resolvidos com fallbacks e shims pontuais (mantém API pública).
- API preservada: registrar_saida_boleto, pagar_parcela_boleto,
  pagar_fatura_cartao/pagar_fatura, programar_emprestimo/pagar_parcela_emprestimo, etc.
"""

from __future__ import annotations

import importlib
import logging
import os
import sys
from typing import Any, Optional, Type

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# sys.path bootstrap (execução via Streamlit/CLI)
# ---------------------------------------------------------------------
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


# =====================================================================
# Utils: resolução dinâmica de símbolos (classes) com fallbacks
# =====================================================================
def _resolve_symbol(mod_names: list[str], attr_candidates: list[str]) -> Optional[Type[Any]]:
    """Tenta obter uma classe pesquisando, na ordem, módulos e nomes de atributos."""
    for mod in mod_names:
        try:
            m = importlib.import_module(mod)
        except Exception:
            continue
        for attr in attr_candidates:
            cls = getattr(m, attr, None)
            if isinstance(cls, type):
                return cls  # type: ignore[return-value]
    return None


def _resolve_mixin(
    primary_mods: list[str],
    primary_attrs: list[str],
    fallback: Optional[Type[Any]] = None,
) -> Type[Any]:
    """Resolve um mixin por nome de módulo/atributo com fallback opcional."""
    cls = _resolve_symbol(primary_mods, primary_attrs)
    return cls or (fallback or type("EmptyMixin", (), {}))


# =====================================================================
# Repositórios (dinâmicos; sem import estático rígido)
# =====================================================================
MovimentacoesRepoType: Optional[Type[Any]] = _resolve_symbol(
    mod_names=[
        "repository.movimentacoes_repository",
        "repository.movimentacoes_bancarias_repository",  # compat antigo
    ],
    attr_candidates=[
        "MovimentacoesRepository",
        "MovimentacoesBancariasRepository",  # compat antigo
    ],
)

CartoesRepoType: Optional[Type[Any]] = _resolve_symbol(
    mod_names=[
        "repository.cartoes_repository",
        "repository.cartoes_credito_repository",  # compat antigo
    ],
    attr_candidates=[
        "CartoesRepository",
        "CartoesCreditoRepository",  # compat antigo
    ],
)

# CAP (obrigatório)
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository  # type: ignore

# Serviço de BOLETO (delegação explícita — NÃO herdamos como mixin)
from services.ledger.service_ledger_boleto import ServiceLedgerBoleto  # type: ignore


# =====================================================================
# Mixins (ordem importa na MRO). Resolução dinâmica para os demais.
# =====================================================================
# Infra
_InfraMixin = _resolve_mixin(
    ["services.ledger.service_ledger_infra", "service_ledger_infra", ".service_ledger_infra"],
    ["_InfraLedgerMixin"],
)

# Helpers CAP (status etc.)
_CapStatusMixin = _resolve_mixin(
    ["services.ledger.service_ledger_cap_helpers", "service_ledger_cap_helpers", ".service_ledger_cap_helpers"],
    ["_CapStatusLedgerMixin"],
)

# Autobaixa
_AutoBaixaMixin = _resolve_mixin(
    ["services.ledger.service_ledger_autobaixa", "service_ledger_autobaixa", ".service_ledger_autobaixa"],
    ["_AutoBaixaLedgerMixin"],
)

# Saídas (UI/Forms → dispatcher)
_SaidasMixin = _resolve_mixin(
    ["services.ledger.service_ledger_saida", "service_ledger_saida", ".service_ledger_saida"],
    ["_SaidasLedgerMixin"],
)

# Crédito (cartão/limite etc.)
_CreditoMixin = _resolve_mixin(
    ["services.ledger.service_ledger_credito", "service_ledger_credito", ".service_ledger_credito"],
    ["_CreditoLedgerMixin"],
)

# Empréstimos
_EmpMixin = _resolve_mixin(
    ["services.ledger.service_ledger_emprestimo", "service_ledger_emprestimo", ".service_ledger_emprestimo"],
    ["_EmprestimoLedgerMixin"],
)

# Faturas (mixin direto ou wrapper para ServiceLedgerFatura)
_FaturaMixin = _resolve_symbol(
    ["services.ledger.service_ledger_fatura", "service_ledger_fatura", ".service_ledger_fatura"],
    ["_FaturaLedgerMixin", "FaturaCartaoMixin"],
)
if _FaturaMixin is None:
    _ServiceFatura = _resolve_symbol(
        ["services.ledger.service_ledger_fatura", "service_ledger_fatura", ".service_ledger_fatura"],
        ["ServiceLedgerFatura"],
    )

    if _ServiceFatura is not None:

        class _FaturaMixin:  # type: ignore
            """Wrapper fino quando só existe ServiceLedgerFatura (sem mixin direto)."""

            # -------- núcleo privado: assinatura canônica esperada pelo ServiceLedgerFatura
            def _pagar_fatura_impl(
                self,
                *,
                obrigacao_id: int,
                principal: float,
                juros: float = 0.0,
                multa: float = 0.0,
                desconto: float = 0.0,
                data_evento: str | None = None,
                usuario: str = "-",
            ) -> dict[str, Any]:
                svc = _ServiceFatura(self.db_path)  # type: ignore[attr-defined]
                return svc.pagar_fatura(
                    obrigacao_id=obrigacao_id,
                    principal=principal,
                    juros=juros,
                    multa=multa,
                    desconto=desconto,
                    data_evento=data_evento,
                    usuario=usuario,
                    conn=None,
                )

            # -------- wrappers públicos com normalização de aliases (compat UI)
            @staticmethod
            def _normalize_fatura_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
                """Normaliza nomes usados na UI para a assinatura canônica do serviço."""
                obrig = (
                    kwargs.get("obrigacao_id")
                    or kwargs.get("obrigacao_id_fatura")
                    or kwargs.get("fatura_id")
                )
                principal = (
                    kwargs.get("principal")
                    if kwargs.get("principal") is not None
                    else (
                        kwargs.get("valor_principal")
                        if kwargs.get("valor_principal") is not None
                        else (
                            kwargs.get("valor_base")
                            if kwargs.get("valor_base") is not None
                            else kwargs.get("valor", 0.0)
                        )
                    )
                )
                data_evt = kwargs.get("data_evento") or kwargs.get("data")
                return {
                    "obrigacao_id": int(obrig or 0),
                    "principal": float(principal or 0.0),
                    "juros": float(kwargs.get("juros") or 0.0),
                    "multa": float(kwargs.get("multa") or 0.0),
                    "desconto": float(kwargs.get("desconto") or 0.0),
                    "data_evento": data_evt,
                    "usuario": (kwargs.get("usuario") or "-"),
                }

            def pagar_fatura(self, **kwargs: Any) -> dict[str, Any]:
                norm = self._normalize_fatura_kwargs(kwargs)
                return self._pagar_fatura_impl(**norm)

            def pagar_fatura_cartao(self, **kwargs: Any) -> dict[str, Any]:
                norm = self._normalize_fatura_kwargs(kwargs)
                return self._pagar_fatura_impl(**norm)

    else:

        class _FaturaMixin:  # vazio: mantém a MRO estável
            pass


# Placeholder para manter a posição do “mixin de boleto” (delegação explícita)
class _BoletoBase:
    """Sentinela para preservar a ordem MRO onde o serviço de boleto é delegado."""


# =====================================================================
# Serviço Agregador
# =====================================================================
class LedgerService(
    _SaidasMixin,
    _CreditoMixin,
    _BoletoBase,  # <- posição reservada; delegação explícita abaixo
    _FaturaMixin,
    _EmpMixin,
    _AutoBaixaMixin,
    _CapStatusMixin,
    _InfraMixin,  # util/base por último
):
    """Fachada central do Ledger, compondo mixins e serviços específicos.

    Mantém API pré-existente:
        - registrar_saida_credito
        - registrar_saida_boleto / pagar_parcela_boleto
        - programar_emprestimo / pagar_parcela_emprestimo
        - pagar_fatura_cartao / pagar_fatura
        - (UI) métodos providos por _SaidasLedgerMixin
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

        # Repositórios aguardados pelos mixins
        self.mov_repo = MovimentacoesRepoType(db_path) if MovimentacoesRepoType else None  # type: ignore
        self.cap_repo = ContasAPagarMovRepository(db_path)
        self.cartoes_repo = CartoesRepoType(db_path) if CartoesRepoType else None  # type: ignore

        # Serviço específico de BOLETO (instanciado sob demanda — evita dependências circulares)
        self._boleto_svc: Optional[ServiceLedgerBoleto] = None

        try:
            super().__init__()  # type: ignore[misc]
        except TypeError:
            pass

        logger.debug("LedgerService inicializado com db_path=%s", self.db_path)

    def __repr__(self) -> str:
        return f"<LedgerService db_path={self.db_path!r}>"

    # ------------------ Ponte/Delegação para BOLETO ------------------
    def _get_boleto(self) -> ServiceLedgerBoleto:
        if self._boleto_svc is None:
            self._boleto_svc = ServiceLedgerBoleto(self.db_path)
        return self._boleto_svc

    def registrar_saida_boleto(
        self,
        *,
        valor: float,
        forma: str | None = None,
        origem: str | None = None,
        banco: str | None = None,
        descricao: str | None = None,
        usuario: str | None = None,
        trans_uid: str | None = None,
        obrigacao_id: int | None = None,
        parcela_id: int | None = None,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        data_evento: str | None = None,
        data_compra: str | None = None,     # compat
        metodo: str | None = None,          # compat (alguns fluxos)
        meio_pagamento: str | None = None,  # compat (alguns fluxos)
        **_ignored: Any,                    # engole kwargs extras
    ) -> dict[str, Any]:
        """Registra saída de BOLETO (avulsa) ou paga parcela via serviço (FIFO)."""
        _data_evt = data_evento or data_compra

        _forma = (forma or metodo or meio_pagamento)
        if not _forma:
            origem_norm = (origem or "").strip().lower()
            if origem_norm in {"caixa", "caixa 2", "caixa2"}:
                _forma = "DINHEIRO"
            elif banco:
                _forma = "PIX"
            else:
                _forma = "DINHEIRO"

        if obrigacao_id is not None:
            boleto = self._get_boleto()
            return boleto.registrar_saida_boleto(
                valor=valor,
                forma=_forma,
                origem=origem,
                banco=banco,
                descricao=descricao,
                usuario=usuario or "-",
                trans_uid=trans_uid,
                obrigacao_id=obrigacao_id,
                parcela_id=parcela_id,
                juros=juros,
                multa=multa,
                desconto=desconto,
                data_evento=_data_evt,
            )

        return self.registrar_lancamento(
            tipo_evento="SAIDA",
            categoria_evento="BOLETO",
            valor_evento=valor,
            forma=_forma,
            origem=origem,
            banco=banco,
            descricao=descricao,
            usuario=usuario,
            trans_uid=trans_uid,
            data_evento=_data_evt,
        )

    # ------------------ Dispatcher seguro para 'registrar_lancamento' ------------------
    def registrar_lancamento(self, **kwargs: Any) -> dict[str, Any]:
        for base in self.__class__.mro()[1:]:
            impl = base.__dict__.get("registrar_lancamento")
            if impl is not None:
                return impl(self, **kwargs)  # type: ignore[misc]
        raise RuntimeError(
            "registrar_lancamento() não está disponível no LedgerService.\n"
            "- Verifique se o mixin de saídas está presente (services.ledger.service_ledger_saida)\n"
            "  e se a classe exporta '_SaidasLedgerMixin' com o método 'registrar_lancamento'.\n"
            "- Alternativa: informe 'obrigacao_id' para usar o fluxo de pagamento de parcela de BOLETO."
        )

    # ------------------ Wrappers compat: BOLETO & EMPRESTIMO ------------------
    def pagar_parcela_boleto(
        self,
        *,
        obrigacao_id: int | None = None,
        obrigacao_id_boleto: int | None = None,  # alias aceito
        parcela_id: int | None = None,           # compat; ignorado (FIFO)
        valor_base: float | None = None,
        valor_principal: float | None = None,    # alias aceito
        valor: float | None = None,              # alias legado
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        usuario: str | None = None,
        data_evento: str | None = None,
        data: str | None = None,                 # alias aceito
        **_kwargs: Any,
    ) -> dict[str, Any]:
        """Compat: delega ao serviço de BOLETO (FIFO). Aceita aliases de IDs/valor/data."""
        _obrig = obrigacao_id if obrigacao_id is not None else (obrigacao_id_boleto or 0)
        _principal = (
            valor_base
            if valor_base is not None
            else (valor_principal if valor_principal is not None else (valor or 0.0))
        )
        _data_evt = data_evento or data

        boleto = self._get_boleto()
        return boleto.pagar_parcela_boleto(
            obrigacao_id=int(_obrig),
            parcela_id=parcela_id,
            valor_base=float(_principal or 0.0),
            juros=float(juros or 0.0),
            multa=float(multa or 0.0),
            desconto=float(desconto or 0.0),
            usuario=usuario or "-",
            data_evento=_data_evt,
        )

    def pagar_parcela_emprestimo(
        self,
        *,
        obrigacao_id_emprestimo: int | None = None,
        obrigacao_id: int | None = None,        # alias aceito
        valor_principal: float | None = None,
        valor_base: float | None = None,        # alias aceito
        valor: float | None = None,             # alias legado
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        usuario: str | None = None,
        data_evento: str | None = None,
        data: str | None = None,                # alias aceito
        **_kwargs: Any,
    ) -> dict[str, Any]:
        """Wrapper compatível para EMPRESTIMO (delegação ao ServiceLedgerEmprestimo)."""
        _obrig = (
            obrigacao_id_emprestimo if obrigacao_id_emprestimo is not None else (obrigacao_id or 0)
        )
        _principal = (
            valor_base
            if valor_base is not None
            else (valor_principal if valor_principal is not None else (valor or 0.0))
        )
        _data_evt = data_evento or data

        try:
            mod = importlib.import_module("services.ledger.service_ledger_emprestimo")
            ServiceLedgerEmprestimo = getattr(mod, "ServiceLedgerEmprestimo")
        except Exception as e:
            raise RuntimeError("ServiceLedgerEmprestimo não encontrado ou inválido.") from e

        svc = ServiceLedgerEmprestimo(self.db_path)
        return svc.pagar_emprestimo(
            obrigacao_id=int(_obrig),
            principal=float(_principal or 0.0),
            juros=float(juros or 0.0),
            multa=float(multa or 0.0),
            desconto=float(desconto or 0.0),
            data_evento=_data_evt,
            usuario=usuario or "-",
            conn=None,
        )


__all__ = ["LedgerService"]
