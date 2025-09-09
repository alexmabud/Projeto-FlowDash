# services/ledger/service_ledger.py
"""
LedgerService
=============

Fachada do Ledger que compõe mixins/serviços e delega as operações:

- Saídas (dinheiro/bancária) via `_SaidasLedgerMixin`
- Crédito / Fatura / Empréstimo / Boleto via serviços específicos
- Repositórios resolvidos dinamicamente (evita acoplamento rígido)

Notas:
- Não reimplementa a lógica de `registrar_saida_*`: apenas delega.
- Aplica fix-up pós-inserção em `saida` (Categoria, Sub_Categoria, Descricao).
- Mantém compat com chamadas antigas e serviços opcionais.
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
# Resolução dinâmica de símbolos (classes)
# =====================================================================
def _resolve_symbol(mod_names: list[str], attr_candidates: list[str]) -> Optional[Type[Any]]:
    """Retorna a primeira classe encontrada dentre módulos/atributos candidatos."""
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


def _resolve_mixin(primary_mods: list[str], primary_attrs: list[str], fallback: Optional[Type[Any]] = None) -> Type[Any]:
    """Resolve um mixin; se não encontrar, retorna um mixin vazio."""
    cls = _resolve_symbol(primary_mods, primary_attrs)
    return cls or (fallback or type("EmptyMixin", (), {}))


# =====================================================================
# Repositórios (dinâmicos; sem import estático rígido)
# =====================================================================
MovimentacoesRepoType: Optional[Type[Any]] = _resolve_symbol(
    ["repository.movimentacoes_repository", "repository.movimentacoes_bancarias_repository"],
    ["MovimentacoesRepository", "MovimentacoesBancariasRepository"],
)

CartoesRepoType: Optional[Type[Any]] = _resolve_symbol(
    ["repository.cartoes_repository", "repository.cartoes_credito_repository"],
    ["CartoesRepository", "CartoesCreditoRepository"],
)

SaidasRepoType: Optional[Type[Any]] = _resolve_symbol(
    ["repository.saidas_repository"],
    ["SaidasRepository"],
)

BancosRepoType: Optional[Type[Any]] = _resolve_symbol(
    ["repository.bancos_repository"],
    ["BancosRepository"],
)

CaixasRepoType: Optional[Type[Any]] = _resolve_symbol(
    ["repository.caixas_repository", "repository.caixa_repository"],
    ["CaixasRepository", "CaixaRepository"],
)

# CAP / Serviços especializados
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository  # type: ignore
from services.ledger.service_ledger_boleto import ServiceLedgerBoleto  # type: ignore

# =====================================================================
# Mixins (ordem importa na MRO)
# =====================================================================
_InfraMixin = _resolve_mixin(
    ["services.ledger.service_ledger_infra", "service_ledger_infra", ".service_ledger_infra"],
    ["_InfraLedgerMixin"],
)

_CapStatusMixin = _resolve_mixin(
    ["services.ledger.service_ledger_cap_helpers", "service_ledger_cap_helpers", ".service_ledger_cap_helpers"],
    ["_CapStatusLedgerMixin"],
)

_AutoBaixaMixin = _resolve_mixin(
    ["services.ledger.service_ledger_autobaixa", "service_ledger_autobaixa", ".service_ledger_autobaixa"],
    ["_AutoBaixaLedgerMixin"],
)

_SaidasMixin = _resolve_mixin(
    ["services.ledger.service_ledger_saida", "service_ledger_saida", ".service_ledger_saida"],
    ["_SaidasLedgerMixin"],
)

_CreditoMixin = _resolve_mixin(
    ["services.ledger.service_ledger_credito", "service_ledger_credito", ".service_ledger_credito"],
    ["_CreditoLedgerMixin"],
)

_EmpMixin = _resolve_mixin(
    ["services.ledger.service_ledger_emprestimo", "service_ledger_emprestimo", ".service_ledger_emprestimo"],
    ["_EmprestimoLedgerMixin", "ServiceLedgerEmprestimo"],  # compat
)

_FaturaMixin = _resolve_symbol(
    ["services.ledger.service_ledger_fatura", "service_ledger_fatura", ".service_ledger_fatura"],
    ["_FaturaLedgerMixin", "FaturaCartaoMixin", "ServiceLedgerFatura"],
) or type("EmptyFaturaMixin", (), {})


class _BoletoBase:
    """Sentinela para preservar a ordem MRO onde o serviço de boleto é delegado."""
    pass


# =====================================================================
# Serviço Agregador
# =====================================================================
class LedgerService(
    _SaidasMixin,
    _CreditoMixin,
    _BoletoBase,
    _FaturaMixin,
    _EmpMixin,
    _AutoBaixaMixin,
    _CapStatusMixin,
    _InfraMixin,
):
    """
    Fachada central do Ledger, compondo mixins/serviços específicos.

    Parâmetros
    ----------
    db_path : str
        Caminho do banco SQLite.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

        # Repositórios aguardados pelos mixins
        self.mov_repo = MovimentacoesRepoType(db_path) if MovimentacoesRepoType else None  # type: ignore
        self.cap_repo = ContasAPagarMovRepository(db_path)
        self.cartoes_repo = CartoesRepoType(db_path) if CartoesRepoType else None  # type: ignore

        # Repositórios auxiliares (saídas/caixa/bancos)
        self.saidas_repo = SaidasRepoType(db_path) if SaidasRepoType else None  # type: ignore
        self.bancos_repo = BancosRepoType(db_path) if BancosRepoType else None  # type: ignore
        self.caixa_repo = CaixasRepoType(db_path) if CaixasRepoType else None  # type: ignore

        self._boleto_svc: Optional[ServiceLedgerBoleto] = None

        # Inicialização dos mixins (se exigirem __init__)
        try:
            super().__init__()  # type: ignore[misc]
        except TypeError:
            pass

    def __repr__(self) -> str:  # pragma: no cover
        return f"<LedgerService db_path={self.db_path!r}>"

    # =========================
    # Helpers locais (pós-inserção)
    # =========================
    @staticmethod
    def _saida__pick_first_existing_col(cols_available: list[str], candidates: list[str]) -> str | None:
        """Retorna a primeira coluna candidata existente na tabela."""
        for c in candidates:
            if c in cols_available:
                return c
        return None

    def _saida__force_fields(
        self,
        *,
        id_saida: int | None,
        categoria: str | None,
        sub_categoria: str | None,
        descricao: str | None,
        forma: str | None,
        set_parcelas_1: bool,
    ) -> None:
        """
        Garante que a linha recém-criada em `saida` possua:
        Categoria / Sub_Categoria / Descricao / (Forma_de_Pagamento se existir)
        e define Parcelas=1 quando aplicável (DINHEIRO/PIX/DÉBITO).
        """
        if not id_saida:
            return
        try:
            import sqlite3
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("PRAGMA table_info(saida);")
                cols = [row[1] for row in cur.fetchall()]

                updates: dict[str, object] = {}

                if categoria is not None:
                    cat_col = self._saida__pick_first_existing_col(
                        cols, ["Categoria", "Categorias", "categoria", "Categoria_Saida"]
                    )
                    if cat_col:
                        updates[cat_col] = categoria

                if sub_categoria is not None:
                    sub_col = self._saida__pick_first_existing_col(
                        cols, ["Sub_Categoria", "Sub_Categorias", "sub_categoria"]
                    )
                    if sub_col:
                        updates[sub_col] = sub_categoria

                if descricao is not None:
                    desc_col = self._saida__pick_first_existing_col(
                        cols, ["Descricao", "Descrição", "descricao"]
                    )
                    if desc_col:
                        updates[desc_col] = descricao

                if forma:
                    forma_col = self._saida__pick_first_existing_col(
                        cols, ["Forma_de_Pagamento", "forma_pagamento", "Forma_Pagamento", "Forma de Pagamento", "forma", "Forma"]
                    )
                    if forma_col:
                        updates.setdefault(forma_col, forma)

                if set_parcelas_1:
                    parc_col = self._saida__pick_first_existing_col(
                        cols, ["Parcelas", "parcelas", "qtd_parcelas", "Qtd_Parcelas", "numero_parcelas", "Numero_Parcelas", "num_parcelas", "n_parcelas"]
                    )
                    if parc_col:
                        updates[parc_col] = 1

                if updates:
                    set_sql = ", ".join([f"{k} = :{k}" for k in updates])
                    updates["__id"] = id_saida
                    cur.execute(f"UPDATE saida SET {set_sql} WHERE id = :__id;", updates)
                    conn.commit()
        except Exception as e:  # pragma: no cover
            logger.warning("Pós-inserção defensivo em 'saida' falhou: %s", e)

    # ------------------ Saídas ------------------
    def registrar_saida_dinheiro(
        self,
        *,
        data: str,
        valor: float,
        origem_dinheiro: str,
        categoria: str | None = None,
        sub_categoria: str | None = None,
        descricao: str | None = None,
        usuario: str | None = None,
        obrigacao_id_fatura: int | None = None,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        trans_uid: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Wrapper compat: delega ao mixin e aplica fix-up."""
        # Campos não suportados pelo mixin
        for k in (
            "parcelas", "qtd_parcelas", "numero_parcelas", "num_parcelas", "n_parcelas",
            "Forma_de_Pagamento", "forma_de_pagamento", "forma_pagamento",
            "banco_saida", "bandeira", "cartao",
            "obrigacao_id_boleto", "obrigacao_id_emprestimo", "obrigacao_id", "tipo_obrigacao",
        ):
            kwargs.pop(k, None)

        result = super().registrar_saida_dinheiro(
            data=data,
            valor=valor,
            origem_dinheiro=origem_dinheiro,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao,
            usuario=(usuario or "-"),
            obrigacao_id_fatura=obrigacao_id_fatura,
            juros=juros,
            multa=multa,
            desconto=desconto,
            trans_uid=trans_uid,
        )

        id_saida = result[0] if isinstance(result, (tuple, list)) and result else None
        self._saida__force_fields(
            id_saida=id_saida,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao,
            forma="DINHEIRO",
            set_parcelas_1=True,  # DINHEIRO ⇒ Parcelas = 1
        )
        return result

    def registrar_saida_bancaria(
        self,
        *,
        data: str,
        valor: float,
        banco_nome: str,
        forma: str,
        categoria: str | None = None,
        sub_categoria: str | None = None,
        descricao: str | None = None,
        usuario: str | None = None,
        obrigacao_id_fatura: int | None = None,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        trans_uid: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Wrapper compat: delega ao mixin e aplica fix-up."""
        for k in (
            "parcelas", "qtd_parcelas", "numero_parcelas", "num_parcelas", "n_parcelas",
            "Forma_de_Pagamento", "forma_de_pagamento", "forma_pagamento",
            "banco_saida", "bandeira", "cartao",
            "obrigacao_id_boleto", "obrigacao_id_emprestimo", "obrigacao_id", "tipo_obrigacao",
        ):
            kwargs.pop(k, None)

        result = super().registrar_saida_bancaria(
            data=data,
            valor=valor,
            banco_nome=banco_nome,
            forma=forma,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao,
            usuario=(usuario or "-"),
            obrigacao_id_fatura=obrigacao_id_fatura,
            juros=juros,
            multa=multa,
            desconto=desconto,
            trans_uid=trans_uid,
        )

        id_saida = result[0] if isinstance(result, (tuple, list)) and result else None
        forma_up = (forma or "").strip().upper()
        self._saida__force_fields(
            id_saida=id_saida,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao,
            forma=forma_up,
            set_parcelas_1=forma_up in {"PIX", "DÉBITO", "DEBITO"},
        )
        return result

    # ------------------ BOLETO ------------------
    def _get_boleto(self) -> ServiceLedgerBoleto:
        """Lazy-init do serviço de boleto (mantém ordem MRO intacta)."""
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
        metodo: str | None = None,          # compat
        meio_pagamento: str | None = None,  # compat
        **_ignored: Any,
    ) -> dict[str, Any]:
        """
        Registra saída de boleto avulsa ou pagamento de parcela (via ServiceLedgerBoleto).
        Quando `obrigacao_id` é fornecido, delega ao serviço de boleto; caso contrário,
        utiliza o dispatcher de lançamentos.
        """
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

        # Sem obrigação → delega ao dispatcher de saídas
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

    # ------------------ Dispatcher seguro ------------------
    def registrar_lancamento(self, **kwargs: Any) -> dict[str, Any]:
        """
        Encaminha para a primeira implementação de `registrar_lancamento` encontrada
        na MRO (mixin de saídas).
        """
        for base in self.__class__.mro()[1:]:
            impl = base.__dict__.get("registrar_lancamento")
            if impl is not None:
                return impl(self, **kwargs)  # type: ignore[misc]
        raise RuntimeError(
            "registrar_lancamento() não está disponível. "
            "Verifique se o mixin de saídas (services.ledger.service_ledger_saida) "
            "está presente e expõe '_SaidasLedgerMixin' com 'registrar_lancamento'."
        )

    # ------------------ Wrappers compat: boleto & empréstimo ------------------
    def pagar_parcela_boleto(
        self,
        *,
        obrigacao_id: int | None = None,
        obrigacao_id_boleto: int | None = None,
        parcela_id: int | None = None,
        valor_base: float | None = None,
        valor_principal: float | None = None,
        valor: float | None = None,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        usuario: str | None = None,
        data_evento: str | None = None,
        data: str | None = None,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        """Wrapper de compatibilidade para pagamento de parcela de boleto."""
        _obrig = obrigacao_id if obrigacao_id is not None else (obrigacao_id_boleto or 0)
        _principal = (
            valor_base if valor_base is not None
            else (valor_principal if valor_principal is not None else (valor or 0.0))
        )
        _data_evt = data_evento or data  # <-- corrigido

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
        obrigacao_id: int | None = None,
        valor_principal: float | None = None,
        valor_base: float | None = None,
        valor: float | None = None,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        usuario: str | None = None,
        data_evento: str | None = None,
        data: str | None = None,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        """Wrapper de compatibilidade para pagamento de parcela de empréstimo."""
        _obrig = obrigacao_id_emprestimo if obrigacao_id_emprestimo is not None else (obrigacao_id or 0)
        _principal = (
            valor_base if valor_base is not None
            else (valor_principal if valor_principal is not None else (valor or 0.0))
        )
        _data_evt = data_evento or data  # <-- corrigido

        try:
            mod = importlib.import_module("services.ledger.service_ledger_emprestimo")
            ServiceLedgerEmprestimo = getattr(mod, "ServiceLedgerEmprestimo")
        except Exception as e:  # pragma: no cover
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
