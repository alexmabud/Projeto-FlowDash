"""
Módulo Pagamentos (Contas a Pagar - Mixins)
===========================================

Este módulo define a classe `PaymentsMixin`, responsável por validações e
registro de eventos de **pagamento** de parcelas (boletos), além de atualização
acumulada em `contas_a_pagar_mov`.

Funcionalidades principais
--------------------------
- Garantir que o pagamento não exceda o saldo em aberto (`vw_cap_saldos`).
- Registrar evento **PAGAMENTO** em boletos.
- Atualizar acumulados da parcela (valor pago, juros, multa, desconto).
- Definir status da parcela (Quitado, Parcial).

Detalhes técnicos
-----------------
- ESTE MIXIN **NÃO** herda de `BaseRepo`. Ele é combinado com `BaseRepo` na
  classe final (`ContasAPagarMovRepository`), que fornece `_get_conn()`,
  `_validar_evento_basico`, `_inserir_evento`, etc.
- Usa `_q2` (quantização decimal) para precisão em valores.
- Eventos de pagamento são registrados como **valores negativos**.
- A atualização acumulada depende de colunas extras no modelo (`valor_pago_acumulado`,
  `juros_pago`, `multa_paga`, `desconto_aplicado`, `status`).

Dependências
------------
- decimal.Decimal
- typing.Optional
- repository.contas_a_pagar_mov_repository.types._q2
"""

from typing import Optional
from decimal import Decimal

# REMOVIDO: from repository.contas_a_pagar_mov_repository.base import BaseRepo
from repository.contas_a_pagar_mov_repository.types import _q2


class PaymentsMixin(object):
    """Operações de pagamento (boletos/faturas/etc.)."""

    def __init__(self, *args, **kwargs):
        # __init__ cooperativo para múltipla herança
        super().__init__(*args, **kwargs)

    def _validar_pagamento_nao_excede_saldo(
        self,
        conn,
        obrigacao_id: int,
        valor_pago: float,
    ) -> float:
        """
        Garante que o pagamento não exceda o saldo em aberto.

        Retorno
        -------
        float
            Saldo atual em aberto.

        Lança
        -----
        ValueError
            Se o pagamento exceder (tolerância de 0,005).
        """
        row = conn.execute(
            "SELECT COALESCE(saldo_aberto,0) FROM vw_cap_saldos WHERE obrigacao_id=?;",
            (int(obrigacao_id),),
        ).fetchone()
        saldo = float(row[0]) if row else 0.0

        valor_pago = float(valor_pago)
        if valor_pago <= 0:
            raise ValueError("O valor do pagamento deve ser positivo.")

        eps = 0.005  # tolerância de centavos
        if valor_pago > saldo + eps:
            raise ValueError(f"Pagamento (R$ {valor_pago:.2f}) maior que o saldo (R$ {saldo:.2f}).")
        return saldo

    def registrar_pagamento_parcela_boleto(
        self,
        conn,
        *,
        obrigacao_id: int,
        valor_pago: float,
        data_evento: str,          # 'YYYY-MM-DD'
        forma_pagamento: str,
        origem: str,               # 'Caixa' / 'Caixa 2' / nome do banco
        ledger_id: int,
        usuario: str,
        descricao_extra: Optional[str] = None,
    ) -> int:
        """
        Insere um evento **PAGAMENTO** (valor_evento negativo) para um boleto.

        Validações
        ----------
        - tipo_obrigacao='BOLETO'
        - não exceder saldo
        - valor positivo informado (será registrado como negativo)

        Retorno
        -------
        int
            ID do evento inserido.
        """
        # 1) valida saldo
        self._validar_pagamento_nao_excede_saldo(conn, int(obrigacao_id), float(valor_pago))

        # 2) validações básicas do evento
        self._validar_evento_basico(
            obrigacao_id=int(obrigacao_id),
            tipo_obrigacao="BOLETO",
            categoria_evento="PAGAMENTO",
            data_evento=data_evento,
            valor_evento=-abs(float(valor_pago)),
            usuario=usuario,
        )

        # 3) insere evento (PAGAMENTO é negativo)
        return self._inserir_evento(
            conn,
            obrigacao_id=int(obrigacao_id),
            tipo_obrigacao="BOLETO",
            categoria_evento="PAGAMENTO",
            data_evento=data_evento,
            vencimento=None,
            valor_evento=-abs(float(valor_pago)),
            descricao=descricao_extra,     # ex.: "Parcela 2/5 — Credor X"
            credor=None,
            competencia=None,
            parcela_num=None,
            parcelas_total=None,
            forma_pagamento=forma_pagamento,
            origem=origem,
            ledger_id=int(ledger_id) if ledger_id is not None else None,
            usuario=usuario,
        )

    def aplicar_pagamento_parcela(
        self,
        conn,
        *,
        parcela_id: int,
        valor_parcela: float,
        valor_pago_total: float,   # total desembolsado agora (já com juros/multa/desconto aplicados)
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
    ) -> dict:
        """
        Atualiza a linha da parcela acumulando pagamento/encargos e define o status.

        Observações
        -----------
        - Só usar se a tabela possuir as colunas extras:
          `valor_pago_acumulado`, `juros_pago`, `multa_paga`, `desconto_aplicado`, `status`.
        - No modelo atual, a UI usa views para status e saldo.

        Retorno
        -------
        dict
            Contém:
            - parcela_id
            - valor_parcela
            - valor_quitacao
            - pago_acumulado
            - status
            - restante
        """
        vp = _q2(valor_parcela)
        pago = _q2(valor_pago_total)
        j = _q2(juros)
        m = _q2(multa)
        d = _q2(desconto)

        valor_quitacao = _q2(vp - d + j + m)

        cur = conn.cursor()
        cur.execute(
            """
            SELECT 
                COALESCE(valor_pago_acumulado,0),
                COALESCE(juros_pago,0),
                COALESCE(multa_paga,0),
                COALESCE(desconto_aplicado,0)
            FROM contas_a_pagar_mov
            WHERE id = ?
            """,
            (parcela_id, ),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Parcela id={parcela_id} não encontrada em contas_a_pagar_mov")

        pago_acum_atual, juros_acum_atual, multa_acum_atual, desc_acum_atual = map(Decimal, map(str, row))

        novo_pago_acum = _q2(pago_acum_atual + pago)
        novo_juros = _q2(juros_acum_atual + j)
        novo_multa = _q2(multa_acum_atual + m)
        novo_desc = _q2(desc_acum_atual + d)

        status = "Quitado" if novo_pago_acum >= valor_quitacao else "Parcial"
        restante = _q2(max(Decimal("0.00"), valor_quitacao - novo_pago_acum))

        cur.execute(
            """
            UPDATE contas_a_pagar_mov
               SET valor_pago_acumulado = ?,
                   juros_pago           = ?,
                   multa_paga           = ?,
                   desconto_aplicado    = ?,
                   status               = ?
             WHERE id = ?
            """,
            (
                float(novo_pago_acum),
                float(novo_juros),
                float(novo_multa),
                float(novo_desc),
                status,
                parcela_id,
            ),
        )
        conn.commit()

        return {
            "parcela_id": parcela_id,
            "valor_parcela": float(vp),
            "valor_quitacao": float(valor_quitacao),
            "pago_acumulado": float(novo_pago_acum),
            "status": status,
            "restante": float(restante),
        }


# API pública explícita
__all__ = ["PaymentsMixin"]
