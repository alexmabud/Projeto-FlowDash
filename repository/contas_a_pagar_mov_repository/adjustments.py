"""
Módulo Ajustes (Contas a Pagar - Mixins)
========================================

Este módulo define a classe `AdjustmentsMixin`, responsável por registrar
eventos de **ajustes financeiros** aplicados a boletos na tabela
`contas_a_pagar_mov`.

Funcionalidades principais
--------------------------
- Registrar **MULTA** em boleto.
- Registrar **JUROS** em boleto.
- Registrar **DESCONTO** em boleto (valor negativo).

Detalhes técnicos
-----------------
- ESTE MIXIN **NÃO** herda de `BaseRepo`. Ele é combinado com `BaseRepo` na
  classe final (`ContasAPagarMovRepository`), que fornece utilitários como
  `_validar_evento_basico` e `_inserir_evento`.
- Eventos registrados sempre usam `tipo_obrigacao='BOLETO'`.
- Desconto é armazenado como valor negativo para reduzir o saldo.

Dependências
------------
- typing.Optional
"""

from typing import Optional


class AdjustmentsMixin(object):
    """Mixin para registrar ajustes de boleto (MULTA, JUROS, DESCONTO)."""

    def __init__(self, *args, **kwargs):
        # __init__ cooperativo para múltipla herança
        super().__init__(*args, **kwargs)

    def registrar_multa_boleto(
        self,
        conn,
        *,
        obrigacao_id: int,
        valor: float,
        data_evento: str,
        usuario: str,
        descricao: Optional[str] = None,
    ) -> int:
        """Registra evento de **MULTA** em boleto."""
        v = float(valor)
        if v <= 0:
            return 0
        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao="BOLETO",
            categoria_evento="MULTA",
            data_evento=data_evento,
            valor_evento=v,
            usuario=usuario,
        )
        return self._inserir_evento(
            conn,
            obrigacao_id=obrigacao_id,
            tipo_obrigacao="BOLETO",
            categoria_evento="MULTA",
            data_evento=data_evento,
            vencimento=None,
            valor_evento=v,
            descricao=descricao,
            credor=None,
            competencia=None,
            parcela_num=None,
            parcelas_total=None,
            forma_pagamento=None,
            origem=None,
            ledger_id=None,
            usuario=usuario,
        )

    def registrar_juros_boleto(
        self,
        conn,
        *,
        obrigacao_id: int,
        valor: float,
        data_evento: str,
        usuario: str,
        descricao: Optional[str] = None,
    ) -> int:
        """Registra evento de **JUROS** em boleto."""
        v = float(valor)
        if v <= 0:
            return 0
        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao="BOLETO",
            categoria_evento="JUROS",
            data_evento=data_evento,
            valor_evento=v,
            usuario=usuario,
        )
        return self._inserir_evento(
            conn,
            obrigacao_id=obrigacao_id,
            tipo_obrigacao="BOLETO",
            categoria_evento="JUROS",
            data_evento=data_evento,
            vencimento=None,
            valor_evento=v,
            descricao=descricao,
            credor=None,
            competencia=None,
            parcela_num=None,
            parcelas_total=None,
            forma_pagamento=None,
            origem=None,
            ledger_id=None,
            usuario=usuario,
        )

    def registrar_desconto_boleto(
        self,
        conn,
        *,
        obrigacao_id: int,
        valor: float,
        data_evento: str,
        usuario: str,
        descricao: Optional[str] = None,
    ) -> int:
        """Registra evento de **DESCONTO** em boleto (valor negativo)."""
        v = float(valor)
        if v <= 0:
            return 0
        # desconto reduz a dívida (evento negativo)
        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao="BOLETO",
            categoria_evento="DESCONTO",
            data_evento=data_evento,
            valor_evento=-v,
            usuario=usuario,
        )
        return self._inserir_evento(
            conn,
            obrigacao_id=obrigacao_id,
            tipo_obrigacao="BOLETO",
            categoria_evento="DESCONTO",
            data_evento=data_evento,
            vencimento=None,
            valor_evento=-v,
            descricao=descricao,
            credor=None,
            competencia=None,
            parcela_num=None,
            parcelas_total=None,
            forma_pagamento=None,
            origem=None,
            ledger_id=None,
            usuario=usuario,
        )


# API pública explícita
__all__ = ["AdjustmentsMixin"]
