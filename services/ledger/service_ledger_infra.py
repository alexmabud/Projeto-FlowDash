
"""Infraestrutra do Ledger (_InfraLedgerMixin).

Responsável por operações utilitárias comuns:
- Garantir linhas em `saldos_caixas` e `saldos_bancos`
- Criar/ajustar colunas dinâmicas para bancos
- Utilidades de data (add_months, competência de cartão)

Estilo: Docstrings Google (pt-BR).
"""
from __future__ import annotations

import pandas as pd
import calendar
from datetime import date, datetime, timedelta
import sqlite3
from typing import Optional

class _InfraLedgerMixin:
    """Mixin com utilitários de infraestrutura para o Ledger."""

    # ---- saldos_caixas / saldos_bancos -------------------------------------------------
    def _garantir_linha_saldos_caixas(self, conn: sqlite3.Connection, data: str) -> None:
        """Garante a existência da linha em `saldos_caixas` para a data.

        Args:
            conn: Conexão SQLite aberta.
            data: Data no formato `YYYY-MM-DD`.
        """
        cur = conn.execute("SELECT 1 FROM saldos_caixas WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("""
                INSERT INTO saldos_caixas (data, caixa, caixa_2, caixa_vendas, caixa2_dia, caixa_total, caixa2_total)
                VALUES (?, 0, 0, 0, 0, 0, 0)
            """, (data,))

    def _garantir_linha_saldos_bancos(self, conn: sqlite3.Connection, data: str) -> None:
        """Garante a existência da linha em `saldos_bancos` para a data.

        Args:
            conn: Conexão SQLite aberta.
            data: Data no formato `YYYY-MM-DD`.
        """
        cur = conn.execute("SELECT 1 FROM saldos_bancos WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("INSERT OR IGNORE INTO saldos_bancos (data) VALUES (?)", (data,))

    def _ajustar_banco_dynamic(self, conn: sqlite3.Connection, banco_col: str, delta: float, data: str) -> None:
        """Ajusta dinamicamente a coluna do banco em `saldos_bancos`.

        Cria a coluna do banco (se necessário) e aplica o delta na data indicada.

        Args:
            conn: Conexão SQLite aberta.
            banco_col: Nome exato da coluna do banco.
            delta: Variação a aplicar (pode ser positiva ou negativa).
            data: Data do ajuste no formato `YYYY-MM-DD`.
        """
        cols = [r[1] for r in conn.execute("PRAGMA table_info(saldos_bancos)").fetchall()]
        if banco_col not in cols:
            conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_col}" REAL DEFAULT 0.0;')
        self._garantir_linha_saldos_bancos(conn, data)
        conn.execute(
            f'UPDATE saldos_bancos SET "{banco_col}" = COALESCE("{banco_col}",0) + ? WHERE data = ?',
            (float(delta), data)
        )

    # ---- data utils ---------------------------------------------------------------------
    @staticmethod
    def _add_months(dt: date, months: int) -> date:
        """Soma meses preservando fim de mês quando aplicável."""
        y = dt.year + (dt.month - 1 + months) // 12
        m = (dt.month - 1 + months) % 12 + 1
        d = min(dt.day, [31,
                        29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28,
                        31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
        return date(y, m, d)

    def _competencia_compra(self, compra_dt: datetime, vencimento_dia: int, dias_fechamento: int) -> str:
        """Calcula a competência da compra (cartão).

        Regra: fechamento = data(vencimento) - dias_fechamento.
        - Compra NO dia de fechamento fica no MÊS ATUAL.
        - Compra DEPOIS do fechamento vai para o PRÓXIMO mês.

        Args:
            compra_dt: Data/hora da compra.
            vencimento_dia: Dia do vencimento do cartão.
            dias_fechamento: Dias antes do vencimento que ocorre o fechamento.

        Returns:
            str: Competência no formato `YYYY-MM`.
        """
        y, m = compra_dt.year, compra_dt.month
        last = calendar.monthrange(y, m)[1]
        venc_d = min(int(vencimento_dia), last)
        venc_date = datetime(y, m, venc_d)
        fechamento_date = venc_date - timedelta(days=int(dias_fechamento))
        if compra_dt > fechamento_date:  # no fechamento fica no mês atual
            if m == 12:
                y += 1; m = 1
            else:
                m += 1
        return f"{y:04d}-{m:02d}"
