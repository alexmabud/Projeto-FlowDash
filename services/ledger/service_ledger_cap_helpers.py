
"""Helpers de contas a pagar (_CapStatusLedgerMixin).

Calcula saldo, total pago e status (Em aberto/Parcial/Quitado) por obrigação.
"""
from __future__ import annotations
import sqlite3

class _CapStatusLedgerMixin:
    """Mixin com helpers para saldos e status em CAP."""

    def _open_predicate_capm(self) -> str:
        return "COALESCE(status, 'Em aberto') = 'Em aberto'"

    def _expr_valor_documento(self, conn: sqlite3.Connection) -> str:
        return "COALESCE(valor_evento, 0)"

    def _expr_valor_pago(self, conn: sqlite3.Connection) -> str:
        return "COALESCE(valor_evento, 0)"

    def _total_pago_acumulado(self, conn: sqlite3.Connection, obrigacao_id: int) -> float:
        cur = conn.cursor()
        soma = cur.execute("""
            SELECT COALESCE(SUM(
                CASE
                    WHEN UPPER(COALESCE(categoria_evento,'')) LIKE 'PAGAMENTO%' THEN -valor_evento
                    ELSE 0
                END
            ), 0)
            FROM contas_a_pagar_mov
            WHERE obrigacao_id = ?
        """, (int(obrigacao_id),)).fetchone()[0]
        return float(soma or 0.0)

    def _saldo_obrigacao(self, conn: sqlite3.Connection, obrigacao_id: int) -> float:
        cur = conn.cursor()
        s = cur.execute("""
            SELECT COALESCE(SUM(COALESCE(valor_evento,0)),0)
            FROM contas_a_pagar_mov
            WHERE obrigacao_id = ?
        """, (int(obrigacao_id),)).fetchone()[0]
        return float(s or 0.0)

    def _tem_pagamento(self, conn: sqlite3.Connection, obrigacao_id: int) -> bool:
        cur = conn.cursor()
        n = cur.execute("""
            SELECT COUNT(1)
            FROM contas_a_pagar_mov
            WHERE obrigacao_id = ?
              AND UPPER(COALESCE(categoria_evento,'')) = 'PAGAMENTO'
              AND COALESCE(valor_evento,0) <> 0
        """, (int(obrigacao_id),)).fetchone()[0]
        return int(n or 0) > 0

    def _atualizar_status_por_id(self, conn: sqlite3.Connection, row_id: int, obrigacao_id: int, _valor_doc_ignorado: float = 0.0) -> None:
        """Atualiza status do LANCAMENTO (por id) com base no saldo agregado da obrigação."""
        eps = 0.005
        saldo = self._saldo_obrigacao(conn, int(obrigacao_id))
        if abs(saldo) <= eps:
            novo = "Quitado"
        else:
            novo = "Parcial" if self._tem_pagamento(conn, int(obrigacao_id)) else "Em aberto"
        conn.execute("UPDATE contas_a_pagar_mov SET status = ? WHERE id = ?", (novo, int(row_id)))

    def _atualizar_status_por_obrigacao(self, conn: sqlite3.Connection, obrigacao_id: int) -> None:
        """Atualiza o status de todos os LANCAMENTOS de uma obrigação."""
        eps = 0.005
        saldo = self._saldo_obrigacao(conn, int(obrigacao_id))
        if abs(saldo) <= eps:
            novo = "Quitado"
        else:
            novo = "Parcial" if self._tem_pagamento(conn, int(obrigacao_id)) else "Em aberto"

        conn.execute("""
            UPDATE contas_a_pagar_mov
               SET status = ?
             WHERE obrigacao_id = ?
               AND categoria_evento = 'LANCAMENTO'
        """, (novo, int(obrigacao_id)))
