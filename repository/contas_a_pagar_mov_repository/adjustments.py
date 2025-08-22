"""
Ajustes de boleto: MULTA, JUROS, DESCONTO.
"""

from typing import Optional
from .base import BaseRepo


class AdjustmentsMixin(BaseRepo):
    def registrar_multa_boleto(self, conn, *, obrigacao_id: int, valor: float, data_evento: str, usuario: str, descricao: Optional[str] = None) -> int:
        v = float(valor)
        if v <= 0:
            return 0
        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao="BOLETO",
            categoria_evento="MULTA",
            data_evento=data_evento,
            valor_evento=v,
            usuario=usuario
        )
        return self._inserir_evento(
            conn,
            obrigacao_id=obrigacao_id, tipo_obrigacao="BOLETO", categoria_evento="MULTA",
            data_evento=data_evento, vencimento=None, valor_evento=v, descricao=descricao,
            credor=None, competencia=None, parcela_num=None, parcelas_total=None,
            forma_pagamento=None, origem=None, ledger_id=None, usuario=usuario
        )

    def registrar_juros_boleto(self, conn, *, obrigacao_id: int, valor: float, data_evento: str, usuario: str, descricao: Optional[str] = None) -> int:
        v = float(valor)
        if v <= 0:
            return 0
        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao="BOLETO",
            categoria_evento="JUROS",
            data_evento=data_evento,
            valor_evento=v,
            usuario=usuario
        )
        return self._inserir_evento(
            conn,
            obrigacao_id=obrigacao_id, tipo_obrigacao="BOLETO", categoria_evento="JUROS",
            data_evento=data_evento, vencimento=None, valor_evento=v, descricao=descricao,
            credor=None, competencia=None, parcela_num=None, parcelas_total=None,
            forma_pagamento=None, origem=None, ledger_id=None, usuario=usuario
        )

    def registrar_desconto_boleto(self, conn, *, obrigacao_id: int, valor: float, data_evento: str, usuario: str, descricao: Optional[str] = None) -> int:
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
            usuario=usuario
        )
        return self._inserir_evento(
            conn,
            obrigacao_id=obrigacao_id, tipo_obrigacao="BOLETO", categoria_evento="DESCONTO",
            data_evento=data_evento, vencimento=None, valor_evento=-v, descricao=descricao,
            credor=None, competencia=None, parcela_num=None, parcelas_total=None,
            forma_pagamento=None, origem=None, ledger_id=None, usuario=usuario
        )
