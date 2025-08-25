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
- NÃO herda de `BaseRepo`. Este mixin é combinado com `BaseRepo` na classe final
  do repositório, que fornece utilitários como `_validar_evento_basico` e
  `_inserir_evento`.
- `TipoObrigacao` é importado de `types` e usado para tipagem.
- Eventos de pagamento e ajuste são armazenados como **valores negativos**.

Dependências
------------
- typing.Optional
- repository.contas_a_pagar_mov_repository.types.TipoObrigacao
"""

from typing import Optional
from repository.contas_a_pagar_mov_repository.types import TipoObrigacao


class EventsMixin(object):
    """Mixin para registrar eventos principais: LANCAMENTO, PAGAMENTO e AJUSTE (legado)."""

    def __init__(self, *args, **kwargs):
        # __init__ cooperativo para múltipla herança
        super().__init__(*args, **kwargs)

    def registrar_lancamento(
        self,
        conn,
        *,
        obrigacao_id: int,
        tipo_obrigacao: TipoObrigacao,
        valor_total: float,
        data_evento: str,          # 'YYYY-MM-DD'
        vencimento: Optional[str], # 'YYYY-MM-DD'
        descricao: Optional[str],
        credor: Optional[str],
        competencia: Optional[str], # 'YYYY-MM'
        parcela_num: Optional[int],
        parcelas_total: Optional[int],
        usuario: str,
    ) -> int:
        """Registra um evento de **LANCAMENTO** (valor positivo)."""
        valor_total = float(valor_total)
        if valor_total <= 0:
            raise ValueError("LANCAMENTO deve ter valor > 0.")
        competencia = competencia or (vencimento[:7] if vencimento else None)

        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao=tipo_obrigacao,
            categoria_evento="LANCAMENTO",
            data_evento=data_evento,
            valor_evento=valor_total,
            usuario=usuario,
        )

        return self._inserir_evento(
            conn,
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

    def registrar_pagamento(
        self,
        conn,
        *,
        obrigacao_id: int,
        tipo_obrigacao: TipoObrigacao,
        valor_pago: float,
        data_evento: str,          # 'YYYY-MM-DD'
        forma_pagamento: str,
        origem: str,
        ledger_id: int,
        usuario: str,
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

        return self._inserir_evento(
            conn,
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
        conn,
        *,
        obrigacao_id: int,
        tipo_obrigacao: TipoObrigacao,
        valor_negativo: float,     # informe valor POSITIVO; será aplicado como negativo
        data_evento: str,
        descricao: Optional[str],
        credor: Optional[str],
        usuario: str,
    ) -> int:
        """
        Registra um evento de **AJUSTE** (legado).

        Uso:
            - Importação de dívidas antigas (já pagas parcialmente).
            - Ajustes manuais.
        Observação:
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

        return self._inserir_evento(
            conn,
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
