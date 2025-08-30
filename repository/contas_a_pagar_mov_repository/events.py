"""
Módulo Eventos (Contas a Pagar - Mixins)
========================================

Este módulo define a classe `EventsMixin`, responsável por registrar eventos
principais em `contas_a_pagar_mov`.

Eventos suportados
------------------
- **LANCAMENTO**: criação da obrigação (positivo).
- **PAGAMENTO**: quitação parcial/total da obrigação (negativo).
- **AJUSTE (LEGADO)**: importação de dívidas antigas ou ajustes manuais (negativo).

Detalhes técnicos
-----------------
- Este mixin é combinado com `BaseRepo` na classe final do repositório, que
  fornece utilitários como `_validar_evento_basico`, `_inserir_evento` e `_get_conn`.
- `TipoObrigacao` é importado de `types` e usado para tipagem.
- Eventos de pagamento e ajuste são armazenados como **valores negativos**.
"""

from __future__ import annotations

from typing import Optional, Any
import inspect
from repository.contas_a_pagar_mov_repository.types import TipoObrigacao


class EventsMixin(object):
    """Mixin para registrar eventos principais: LANCAMENTO, PAGAMENTO e AJUSTE (legado)."""

    def __init__(self, *args, **kwargs) -> None:
        # __init__ cooperativo para múltipla herança
        super().__init__(*args, **kwargs)

    # ---------------------------------------------------------------------
    # Helpers internos
    # ---------------------------------------------------------------------
    def _conn_ctx(self, conn: Any):
        """
        Context manager de conexão:
        - Se `conn` for fornecido (sqlite3.Connection), usa-o diretamente.
        - Se `conn` for None, abre via `self._get_conn()` (fornecido por BaseRepo).
        """
        if conn is not None:
            class _DummyCtx:
                def __init__(self, c): self.c = c
                def __enter__(self): return self.c
                def __exit__(self, exc_type, exc, tb): return False
            return _DummyCtx(conn)
        # BaseRepo deve fornecer _get_conn()
        return self._get_conn()  # type: ignore[attr-defined]

    def _supports_param(self, fn: Any, name: str) -> bool:
        """Verifica se `fn` aceita um parâmetro chamado `name`."""
        try:
            return name in inspect.signature(fn).parameters
        except Exception:
            return False

    # ---------------------------------------------------------------------
    # Eventos
    # ---------------------------------------------------------------------
    def registrar_lancamento(
        self,
        conn: Any = None,
        *,
        obrigacao_id: int,
        tipo_obrigacao: TipoObrigacao,
        valor_total: float,
        data_evento: str,           # 'YYYY-MM-DD'
        vencimento: Optional[str],  # 'YYYY-MM-DD'
        descricao: Optional[str],
        credor: Optional[str],
        competencia: Optional[str], # 'YYYY-MM'
        parcela_num: Optional[int],
        parcelas_total: Optional[int],
        usuario: str,
        # >>> Correção: aceitar documento (opcional)
        documento: Optional[str] = None,
        # >>> Tolerância a extras para não quebrar chamadas antigas/variantes
        **_extra: Any,
    ) -> int:
        """Registra um evento de **LANCAMENTO** (valor positivo)."""
        valor_total = float(valor_total)
        if valor_total <= 0:
            raise ValueError("LANCAMENTO deve ter valor > 0.")
        # Deriva competência de vencimento (AAAA-MM), se não informada
        competencia = competencia or (vencimento[:7] if vencimento else None)

        self._validar_evento_basico(  # BaseRepo
            obrigacao_id=obrigacao_id,
            tipo_obrigacao=tipo_obrigacao,
            categoria_evento="LANCAMENTO",
            data_evento=data_evento,
            valor_evento=valor_total,
            usuario=usuario,
        )

        with self._conn_ctx(conn) as c:
            # Monta kwargs básicos
            kwargs = dict(
                obrigacao_id=obrigacao_id,
                tipo_obrigacao=tipo_obrigacao,
                categoria_evento="LANCAMENTO",
                data_evento=data_evento,
                vencimento=vencimento,
                valor_evento=valor_total,
                descricao=descricao,
                credor=credor,
                competencia=competencia,
                parcela_num=parcela_num,
                parcelas_total=parcelas_total,
                forma_pagamento=None,
                origem=None,
                ledger_id=None,
                usuario=usuario,
            )
            # Passa documento somente se _inserir_evento suportar
            if documento is not None and self._supports_param(self._inserir_evento, "documento"):
                kwargs["documento"] = documento

            # Chamada final
            return self._inserir_evento(c, **kwargs)  # BaseRepo

    def registrar_pagamento(
        self,
        conn: Any = None,
        *,
        obrigacao_id: int,
        tipo_obrigacao: TipoObrigacao,
        valor_pago: float,
        data_evento: str,          # 'YYYY-MM-DD'
        forma_pagamento: str,
        origem: str,
        ledger_id: int,
        usuario: str,
        **_extra: Any,
    ) -> int:
        """Registra um evento de **PAGAMENTO** (valor negativo)."""
        valor_pago = float(valor_pago)
        if valor_pago <= 0:
            raise ValueError("valor_pago deve ser > 0 para PAGAMENTO.")

        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao=tipo_obrigacao,
            categoria_evento="PAGAMENTO",
            data_evento=data_evento,
            valor_evento=-valor_pago,
            usuario=usuario,
        )

        with self._conn_ctx(conn) as c:
            return self._inserir_evento(
                c,
                obrigacao_id=obrigacao_id,
                tipo_obrigacao=tipo_obrigacao,
                categoria_evento="PAGAMENTO",
                data_evento=data_evento,
                vencimento=None,
                valor_evento=-abs(valor_pago),  # sempre negativo
                descricao=None,
                credor=None,
                competencia=None,
                parcela_num=None,
                parcelas_total=None,
                forma_pagamento=forma_pagamento,
                origem=origem,
                ledger_id=int(ledger_id),
                usuario=usuario,
            )

    def registrar_ajuste_legado(
        self,
        conn: Any = None,
        *,
        obrigacao_id: int,
        tipo_obrigacao: TipoObrigacao,
        valor_negativo: float,     # informe valor POSITIVO; será aplicado como negativo
        data_evento: str,
        descricao: Optional[str],
        credor: Optional[str],
        usuario: str,
        **_extra: Any,
    ) -> int:
        """
        Registra um evento de **AJUSTE** (legado).

        Uso:
            - Importação de dívidas antigas (já pagas parcialmente).
            - Ajustes manuais.
        Observações:
            - Valor sempre registrado como negativo.
            - Não movimenta caixa (ledger_id=None).
        """
        valor_negativo = float(valor_negativo)
        if valor_negativo <= 0:
            raise ValueError("valor_negativo deve ser > 0 (será gravado como negativo).")

        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao=tipo_obrigacao,
            categoria_evento="AJUSTE",
            data_evento=data_evento,
            valor_evento=-valor_negativo,
            usuario=usuario,
        )

        with self._conn_ctx(conn) as c:
            return self._inserir_evento(
                c,
                obrigacao_id=obrigacao_id,
                tipo_obrigacao=tipo_obrigacao,
                categoria_evento="AJUSTE",
                data_evento=data_evento,
                vencimento=None,
                valor_evento=-abs(valor_negativo),
                descricao=descricao,
                credor=credor,
                competencia=None,
                parcela_num=None,
                parcelas_total=None,
                forma_pagamento="LEGADO",
                origem="IMPORTACAO",
                ledger_id=None,
                usuario=usuario,
            )


# API pública explícita
__all__ = ["EventsMixin"]
