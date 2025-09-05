# services/ledger/service_ledger_cap_helpers.py
"""
Helpers de Contas a Pagar (CAP) — padrão novo

Regras centrais:
- STATUS e FALTANTE dependem SOMENTE de principal_pago_acumulado vs valor_evento.
- valor_pago_acumulado (no CAP) é BRUTO: principal + DESCONTO + juros + multa.
  *Esse campo é apenas auditoria de “tamanho do pagamento”; não define status/faltante.*
- Dinheiro que sai do caixa/banco é controlado em movimentacoes_bancarias (saida_total),
  e segue: saida_total = principal + juros + multa (desconto NÃO sai do caixa).
- Encargos acumulados: juros_pago_acumulado, multa_paga_acumulada, desconto_aplicado_acumulado.
- Coluna legada `valor`: ignorada.

APIs (retrocompat mantida onde possível):
- _expr_valor_documento(conn) -> str
- _expr_valor_pagamento(conn) -> str
- _total_pago_acumulado(conn, obrigacao_id) -> float        # soma de principal_pago_acumulado
- _saldo_agregado_por_obrigacao(conn, obrigacao_id) -> float # Σ(valor_evento) − Σ(principal_pago_acumulado)
- _atualizar_status_por_id(conn, parcela_id, ...) -> str
- _atualizar_status_por_obrigacao(conn, obrigacao_id) -> str
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from typing import Optional, Iterator

logger = logging.getLogger(__name__)

STATUS_ABERTO = "EM ABERTO"
STATUS_PARCIAL = "PARCIAL"
STATUS_QUITADO = "QUITADO"

_EPS = 1e-9  # tolerância numérica para comparações de status


class _CapHelpersLedgerMixin:
    """Helpers utilitários para CAP (saldo, total pago, status) no padrão novo."""

    # ------------------------ conexão ------------------------
    @contextmanager
    def _conn_ctx(self, conn: Optional[sqlite3.Connection]) -> Iterator[sqlite3.Connection]:
        """Garante `row_factory=sqlite3.Row` dentro do escopo.

        Se `conn` for fornecida, apenas aplica/recupera `row_factory` no bloco.
        Caso contrário, abre/commita/fecha automaticamente uma nova conexão.

        Args:
            conn: Conexão SQLite existente (opcional).

        Yields:
            Conexão SQLite com `row_factory` configurado para `sqlite3.Row`.
        """
        if conn is not None:
            old = getattr(conn, "row_factory", None)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.row_factory = old
        else:
            c = sqlite3.connect(self.db_path)  # type: ignore[attr-defined]
            c.row_factory = sqlite3.Row
            try:
                yield c
            finally:
                c.commit()
                c.close()

    # ------------------------ expressões SQL ------------------
    def _expr_valor_documento(self, conn: sqlite3.Connection) -> str:  # noqa: ARG002 (assinatura compat)
        """Expressão SQL do valor do documento (principal do LANCAMENTO)."""
        return "COALESCE(valor_evento,0)"

    def _expr_valor_pagamento(self, conn: sqlite3.Connection) -> str:  # noqa: ARG002 (assinatura compat)
        """(Legado) Expressão SQL do valor do evento de pagamento (linhas 'PAGAMENTO')."""
        return "ABS(COALESCE(valor_evento,0))"

    # ------------------------ cálculos agregados --------------
    def _total_pago_acumulado(self, conn: sqlite3.Connection, obrigacao_id: int) -> float:
        """Soma do PRINCIPAL amortizado (`principal_pago_acumulado`) na obrigação.

        OBS: Antes alguns fluxos somavam `valor_pago_acumulado` (BRUTO). Agora
        usamos somente o principal para total pago em termos de quitação.
        """
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            row = cur.execute(
                """
                SELECT COALESCE(SUM(principal_pago_acumulado), 0.0)
                  FROM contas_a_pagar_mov
                 WHERE categoria_evento = 'LANCAMENTO'
                   AND obrigacao_id = ?
                """,
                (int(obrigacao_id),),
            ).fetchone()
            return float(row[0] or 0.0)

    def _saldo_agregado_por_obrigacao(self, conn: sqlite3.Connection, obrigacao_id: int) -> float:
        """Calcula o saldo (faltante) agregado de PRINCIPAL da obrigação.

        Fórmula: Σ(valor_evento) - Σ(principal_pago_acumulado)
        """
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            row = cur.execute(
                """
                SELECT
                    COALESCE(SUM(valor_evento), 0.0)             AS total_doc,
                    COALESCE(SUM(principal_pago_acumulado), 0.0) AS total_principal
                  FROM contas_a_pagar_mov
                 WHERE categoria_evento = 'LANCAMENTO'
                   AND obrigacao_id = ?
                """,
                (int(obrigacao_id),),
            ).fetchone()
            total_doc = float(row["total_doc"] or 0.0)
            total_princ = float(row["total_principal"] or 0.0)
            return round(max(0.0, total_doc - total_princ), 2)

    # ------------------------ status helpers ------------------
    def _status_from_vals(self, valor_evento: float, principal_pago: float) -> str:
        """Determina o status apenas pelo principal amortizado."""
        if (principal_pago or 0.0) >= (valor_evento or 0.0) - _EPS:
            return STATUS_QUITADO
        if (principal_pago or 0.0) > 0:
            return STATUS_PARCIAL
        return STATUS_ABERTO

    def _atualizar_status_por_id(
        self,
        conn: sqlite3.Connection,
        parcela_id: int,
        *args,
        **kwargs,
    ) -> str:
        """Atualiza o STATUS de **uma** parcela `LANCAMENTO` pelo PRINCIPAL.

        Retrocompat:
            Aceita formas antigas com parâmetros posicionais/nominais, por exemplo:
              _atualizar_status_por_id(conn, parcela_id, valor_evento, vpa, desconto)
              _atualizar_status_por_id(conn, parcela_id, obrigacao_id, valor_evento)
            E kwargs como:
              valor_evento=..., principal_pago_acumulado=... (ou vpa/valor_pago_acumulado para legado)

        Returns:
            Novo status da parcela (EM ABERTO | PARCIAL | QUITADO).
        """
        with self._conn_ctx(conn) as c:
            cur = c.cursor()

            base = cur.execute(
                """
                SELECT obrigacao_id, valor_evento, principal_pago_acumulado
                  FROM contas_a_pagar_mov
                 WHERE id = ? AND categoria_evento = 'LANCAMENTO'
                """,
                (int(parcela_id),),
            ).fetchone()
            if not base:
                raise ValueError(f"Lançamento id={parcela_id} não encontrado.")

            obrig_id_bd = int(base["obrigacao_id"])

            # Preferência: kwargs informados
            valor_evento = kwargs.get("valor_evento")
            principal = kwargs.get(
                "principal_pago_acumulado",
                kwargs.get("valor_pago_acumulado", kwargs.get("vpa")),  # retrocompat
            )

            # Interpretação de args posicionais (retrocompat)
            if len(args) == 3:
                # (valor_evento, vpa, desconto) -> usa args[1] como principal
                valor_evento = args[0] if valor_evento is None else valor_evento
                principal = args[1] if principal is None else principal
            elif len(args) == 2:
                # (obrigacao_id, valor_evento) OU (valor_evento, vpa)
                try:
                    if int(args[0]) == obrig_id_bd:
                        valor_evento = args[1] if valor_evento is None else valor_evento
                    else:
                        valor_evento = args[0] if valor_evento is None else valor_evento
                        principal = args[1] if principal is None else principal
                except Exception:
                    valor_evento = args[0] if valor_evento is None else valor_evento
                    principal = args[1] if principal is None else principal
            elif len(args) == 1:
                valor_evento = args[0] if valor_evento is None else valor_evento

            # Completar com BD quando faltar
            if valor_evento is None:
                valor_evento = float(base["valor_evento"] or 0.0)
            if principal is None:
                principal = float(base["principal_pago_acumulado"] or 0.0)

            novo = self._status_from_vals(float(valor_evento or 0.0), float(principal or 0.0))
            cur.execute("UPDATE contas_a_pagar_mov SET status = ? WHERE id = ?", (novo, int(parcela_id)))
            logger.debug("Status por id (novo padrão): id=%s => %s", parcela_id, novo)
            return novo

    def _atualizar_status_por_obrigacao(self, conn: sqlite3.Connection, obrigacao_id: int) -> str:
        """Recalcula e atualiza o STATUS de todas as linhas `LANCAMENTO` da obrigação.

        Regras:
            - Usa apenas `valor_evento` e `principal_pago_acumulado` de cada parcela.
            - Retorna o status agregado final:
                • QUITADO se todas QUITADO
                • PARCIAL se qualquer PARCIAL (ou mistura)
                • EM ABERTO se todas EM ABERTO
        """
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            rows = cur.execute(
                """
                SELECT id, valor_evento, principal_pago_acumulado
                  FROM contas_a_pagar_mov
                 WHERE categoria_evento = 'LANCAMENTO'
                   AND obrigacao_id = ?
                """,
                (int(obrigacao_id),),
            ).fetchall()
            if not rows:
                raise ValueError(f"Obrigação {obrigacao_id} não encontrada.")

            statuses: list[str] = []
            for r in rows:
                novo = self._status_from_vals(
                    float(r["valor_evento"] or 0.0),
                    float(r["principal_pago_acumulado"] or 0.0),
                )
                statuses.append(novo)
                cur.execute("UPDATE contas_a_pagar_mov SET status = ? WHERE id = ?", (novo, int(r["id"])) )

            if all(s == STATUS_QUITADO for s in statuses):
                agregado = STATUS_QUITADO
            elif any(s == STATUS_PARCIAL for s in statuses):
                agregado = STATUS_PARCIAL
            elif all(s == STATUS_ABERTO for s in statuses):
                agregado = STATUS_ABERTO
            else:
                agregado = STATUS_PARCIAL

            logger.debug(
                "Status por obrigacao (novo padrão): obrig=%s => %s (itens=%s)",
                obrigacao_id, agregado, statuses
            )
            return agregado


# --- Retrocompat: manter nome antigo esperado por imports legados ---
class _CapStatusLedgerMixin(_CapHelpersLedgerMixin):
    """Alias de compatibilidade para código antigo."""
    pass


__all__ = [
    "_CapHelpersLedgerMixin",
    "_CapStatusLedgerMixin",
    "STATUS_ABERTO",
    "STATUS_PARCIAL",
    "STATUS_QUITADO",
]
