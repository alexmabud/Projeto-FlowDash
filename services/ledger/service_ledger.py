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
    """Tenta obter uma classe pesquisando, na ordem, módulos e nomes de atributos.

    Percorre a lista de módulos `mod_names` e tenta importá-los. Para cada módulo
    importado com sucesso, retorna o primeiro atributo cujo nome esteja em
    `attr_candidates` e seja uma classe (``type``).

    Args:
        mod_names: Lista de nomes de módulos para tentar importar (em ordem de preferência).
        attr_candidates: Lista de nomes de atributos (classes) a buscar nos módulos.

    Returns:
        A classe encontrada ou ``None`` caso nenhuma combinação (módulo, atributo) seja válida.
    """
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
    """Resolve um mixin por nome de módulo/atributo com fallback opcional.

    Args:
        primary_mods: Módulos onde o mixin pode estar definido.
        primary_attrs: Nomes de classe candidatos para o mixin.
        fallback: Classe de fallback quando nada é encontrado.

    Returns:
        Uma classe (mixin) válida para compor a MRO.
    """
    cls = _resolve_symbol(primary_mods, primary_attrs)
    return cls or (fallback or type("EmptyMixin", (), {}))


# =====================================================================
# Repositórios (dinâmicos; sem import estático rígido)
# =====================================================================
# Movimentações: preferir o nome novo; aceitar legado
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

# Cartões: preferir o nome novo; aceitar legado
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
                """Encaminha a chamada para ServiceLedgerFatura.pagar_fatura()."""
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

            def pagar_fatura(self, **kwargs: Any) -> dict[str, Any]:
                """Alias compatível para pagar fatura."""
                return self._pagar_fatura_impl(**kwargs)

            def pagar_fatura_cartao(self, **kwargs: Any) -> dict[str, Any]:
                """Alias compatível alternativo para pagar fatura."""
                return self._pagar_fatura_impl(**kwargs)

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
        """Inicializa a fachada do Ledger.

        Args:
            db_path: Caminho do arquivo SQLite (banco de dados).
        """
        self.db_path = db_path

        # Repositórios aguardados pelos mixins
        self.mov_repo = MovimentacoesRepoType(db_path) if MovimentacoesRepoType else None  # type: ignore
        self.cap_repo = ContasAPagarMovRepository(db_path)
        self.cartoes_repo = CartoesRepoType(db_path) if CartoesRepoType else None  # type: ignore

        # Serviço específico de BOLETO (instanciado sob demanda — evita dependências circulares)
        self._boleto_svc: Optional[ServiceLedgerBoleto] = None

        # Cooperativo, caso algum mixin possua __init__
        try:
            super().__init__()  # type: ignore[misc]
        except TypeError:
            pass

        logger.debug("LedgerService inicializado com db_path=%s", self.db_path)

    def __repr__(self) -> str:
        return f"<LedgerService db_path={self.db_path!r}>"

    # ------------------ Ponte/Delegação para BOLETO ------------------
    def _get_boleto(self) -> ServiceLedgerBoleto:
        """Instancia lazy o serviço de boleto para evitar dependências circulares.

        Returns:
            Instância de :class:`ServiceLedgerBoleto`.
        """
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
        **_ignored: Any,                    # engole kwargs extras (ex.: documento, parcelas)
    ) -> dict[str, Any]:
        """Registra saída de BOLETO (avulsa) ou paga parcela via serviço (FIFO).

        Comportamento:
            - Com `obrigacao_id`: delega pagamento de parcela (FIFO) ao :class:`ServiceLedgerBoleto`.
            - Sem `obrigacao_id`: registra SAÍDA avulsa com categoria 'BOLETO' via `registrar_lancamento`.
            - Aceita `data_compra`, `metodo`, `meio_pagamento` e ignora kwargs desconhecidos.
            - Se `forma` não vier, infere: origem 'Caixa/Caixa 2' → "DINHEIRO"; senão, se há `banco` → "PIX";
              caso contrário → "DINHEIRO".

        Args:
            valor: Valor do evento (principal quando pago via obrigação).
            forma: Forma de pagamento (opcional; pode ser inferida).
            origem: Origem do recurso ("Caixa", "Caixa 2", ou nome do banco/conta).
            banco: Nome do banco quando aplicável.
            descricao: Descrição legada da saída.
            usuario: Usuário operador.
            trans_uid: Token idempotente legada (não obrigatório aqui).
            obrigacao_id: Identificador da obrigação (grupo de parcelas). Se informado, aciona pagamento FIFO.
            parcela_id: Ignorado no fluxo FIFO (apenas compat).
            juros: Juros a aplicar na primeira parcela aberta (pagamento via obrigação).
            multa: Multa a aplicar na primeira parcela aberta (pagamento via obrigação).
            desconto: Desconto a aplicar na primeira parcela aberta (pagamento via obrigação).
            data_evento: Data do evento (YYYY-MM-DD).
            data_compra: Alias de compatibilidade para data do evento.
            metodo: Alias de compatibilidade para forma de pagamento.
            meio_pagamento: Alias de compatibilidade para forma de pagamento.
            **_ignored: Parâmetros legados ignorados.

        Returns:
            Dicionário com o resultado da operação. No caso de pagamento de parcela,
            retorna a estrutura do serviço de boleto (`trans_uid`, `saida_total`, `resultados`).
        """
        # Normaliza data
        _data_evt = data_evento or data_compra

        # Normaliza/infere forma
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
            # Pagamento de parcela (delegado ao serviço de boleto). 'forma' não é usada na lógica de CAP.
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

        # Saída AVULSA (sem CAP) — usa o registrador genérico do Ledger
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
        """Despacha para a primeira implementação real de `registrar_lancamento` na MRO.

        Vasculha a MRO (exceto a própria classe) procurando uma implementação concreta
        de `registrar_lancamento`. Ao encontrar, delega a chamada e retorna o resultado.

        Args:
            **kwargs: Parâmetros a serem repassados para a implementação encontrada.

        Returns:
            Dicionário com o resultado da operação (conforme implementação do mixin de saídas).

        Raises:
            RuntimeError: Caso nenhum mixin com `registrar_lancamento` esteja presente.
        """
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

    def pagar_parcela_boleto(
        self,
        *,
        obrigacao_id: int,
        parcela_id: int | None = None,  # compat; ignorado (pagamento é FIFO)
        valor_base: float,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        usuario: str | None = None,
        data_evento: str | None = None,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        """Alias compatível — delega ao serviço de boleto (FIFO dentro da obrigação).

        Args:
            obrigacao_id: Identificador da obrigação (grupo de parcelas).
            parcela_id: Ignorado — mantido apenas para compatibilidade.
            valor_base: Valor de principal a aplicar.
            juros: Juros a aplicar (1ª parcela aberta).
            multa: Multa a aplicar (1ª parcela aberta).
            desconto: Desconto a aplicar (1ª parcela aberta).
            usuario: Usuário operador.
            data_evento: Data do evento (YYYY-MM-DD).
            **_kwargs: Parâmetros legados ignorados.

        Returns:
            Estrutura retornada por :meth:`ServiceLedgerBoleto.pagar_parcela_boleto`.
        """
        boleto = self._get_boleto()
        return boleto.pagar_parcela_boleto(
            obrigacao_id=obrigacao_id,
            parcela_id=parcela_id,
            valor_base=valor_base,
            juros=juros,
            multa=multa,
            desconto=desconto,
            usuario=usuario or "-",
            data_evento=data_evento,
        )


__all__ = ["LedgerService"]
