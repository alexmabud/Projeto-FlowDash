"""
service_ledger_cap_helpers.py — Helpers de Contas a Pagar (CAP).

Resumo:
    Cálculo de saldo agregado por obrigação, total pago acumulado e
    atualização automática de status ("Em aberto" / "Parcial" / "Quitado").

Responsabilidades:
    - Expressões SQL utilitárias para CAP (valor do documento/pagamento).
    - Cálculo de total pago acumulado por obrigacao_id.
    - Cálculo de saldo agregado (soma de eventos) por obrigacao_id.
    - Atualização de status de LANCAMENTOS (por id e por obrigacao_id).

Depende de:
    - sqlite3 (conexão gerenciada pelo chamador)
    - Tabela: contas_a_pagar_mov (colunas: id, obrigacao_id, categoria_evento, valor_evento, status)

Notas:
    - Epsilon (eps) usado para mitigar erros de ponto flutuante ao determinar quitação.
    - SQL parametrizado (sem interpolar dados do usuário).
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Imports + bootstrap de caminho (robusto em execuções via Streamlit)
# -----------------------------------------------------------------------------
import logging
import sqlite3
from typing import Final

import os
import sys

# Garante que a raiz do projeto entre no sys.path (…/services/ledger/ -> raiz)
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

logger = logging.getLogger(__name__)

__all__ = ["_CapStatusLedgerMixin"]


class _CapStatusLedgerMixin:
    """Mixin com helpers para saldos e status em CAP."""

    _EPS: Final[float] = 0.005  # tolerância p/ comparações de float

    # ----------------------------------------------------------------------
    # Expressões utilitárias (constantes/derivadas)
    # ----------------------------------------------------------------------
    def _open_predicate_capm(self) -> str:
        """Predicado SQL para itens 'em aberto' (usar em WHERE quando aplicável)."""
        return "COALESCE(status, 'Em aberto') = 'Em aberto'"

    def _expr_valor_documento(self, conn: sqlite3.Connection) -> str:
        """
        Expressão SQL (CONST) para o valor do documento (LANCAMENTO).
        Importante: deve retornar APENAS uma expressão constante/segura.
        """
        return "COALESCE(valor_evento, 0)"

    def _expr_valor_pago(self, conn: sqlite3.Connection) -> str:
        """
        Expressão SQL (CONST) para o valor de pagamento (eventos PAGAMENTO*).
        Mantida separada para evolução futura (descontos/ajustes).
        """
        return "COALESCE(valor_evento, 0)"

    # ----------------------------------------------------------------------
    # Cálculos agregados
    # ----------------------------------------------------------------------
    def _total_pago_acumulado(self, conn: sqlite3.Connection, obrigacao_id: int) -> float:
        """
        Soma de pagamentos para a obrigação.
        Convenção: eventos de pagamento são lançados com valor_evento NEGATIVO;
        por isso somamos -valor_evento para obter o total pago (> 0).
        """
        cur = conn.cursor()
        soma = cur.execute(
            """
            SELECT COALESCE(SUM(
                CASE
                    WHEN UPPER(COALESCE(categoria_evento,'')) LIKE 'PAGAMENTO%' THEN -COALESCE(valor_evento,0)
                    ELSE 0
                END
            ), 0)
            FROM contas_a_pagar_mov
            WHERE obrigacao_id = ?
            """,
            (int(obrigacao_id),),
        ).fetchone()[0]
        total = float(soma or 0.0)
        total = 0.0 if abs(total) < self._EPS else round(total, 2)
        return total

    def _saldo_obrigacao(self, conn: sqlite3.Connection, obrigacao_id: int) -> float:
        """
        Saldo agregado (soma de TODOS os eventos da obrigação).
        Quitado quando o saldo está próximo de zero (|saldo| <= eps).
        """
        cur = conn.cursor()
        s = cur.execute(
            """
            SELECT COALESCE(SUM(COALESCE(valor_evento,0)),0)
            FROM contas_a_pagar_mov
            WHERE obrigacao_id = ?
            """,
            (int(obrigacao_id),),
        ).fetchone()[0]
        saldo = float(s or 0.0)
        saldo = 0.0 if abs(saldo) <= self._EPS else round(saldo, 2)
        return saldo

    def _tem_pagamento(self, conn: sqlite3.Connection, obrigacao_id: int) -> bool:
        """
        Indica se já existe ao menos um evento de pagamento diferente de zero.
        """
        cur = conn.cursor()
        n = cur.execute(
            """
            SELECT COUNT(1)
              FROM contas_a_pagar_mov
             WHERE obrigacao_id = ?
               AND UPPER(COALESCE(categoria_evento,'')) LIKE 'PAGAMENTO%'
               AND COALESCE(valor_evento,0) <> 0
            """,
            (int(obrigacao_id),),
        ).fetchone()[0]
        return int(n or 0) > 0

    # ----------------------------------------------------------------------
    # Atualização de status
    # ----------------------------------------------------------------------
    def _atualizar_status_por_id(
        self,
        conn: sqlite3.Connection,
        row_id: int,
        obrigacao_id: int,
        _valor_doc_ignorado: float = 0.0,
    ) -> None:
        """
        Atualiza o status do LANCAMENTO (por id) com base no saldo agregado da obrigação.
        Regras:
            - |saldo| <= eps  -> "Quitado"
            - saldo != 0 e tem pagamento -> "Parcial"
            - caso contrário -> "Em aberto"
        """
        saldo = self._saldo_obrigacao(conn, int(obrigacao_id))
        if abs(saldo) <= self._EPS:
            novo = "Quitado"
        else:
            novo = "Parcial" if self._tem_pagamento(conn, int(obrigacao_id)) else "Em aberto"

        conn.execute("UPDATE contas_a_pagar_mov SET status = ? WHERE id = ?", (novo, int(row_id)))
        logger.debug(
            "Status por id: id=%s obrig=%s saldo=%.2f => %s",
            row_id, obrigacao_id, saldo, novo
        )

    def _atualizar_status_por_obrigacao(self, conn: sqlite3.Connection, obrigacao_id: int) -> None:
        """
        Atualiza o status de TODOS os LANCAMENTOS de uma obrigação.
        """
        saldo = self._saldo_obrigacao(conn, int(obrigacao_id))
        if abs(saldo) <= self._EPS:
            novo = "Quitado"
        else:
            novo = "Parcial" if self._tem_pagamento(conn, int(obrigacao_id)) else "Em aberto"

        conn.execute(
            """
            UPDATE contas_a_pagar_mov
               SET status = ?
             WHERE obrigacao_id = ?
               AND categoria_evento = 'LANCAMENTO'
            """,
            (novo, int(obrigacao_id)),
        )
        logger.debug(
            "Status por obrigacao: obrig=%s saldo=%.2f => %s",
            obrigacao_id, saldo, novo
        )
