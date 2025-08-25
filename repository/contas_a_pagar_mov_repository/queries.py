"""
Módulo Consultas (Contas a Pagar - Mixins)
==========================================

Este módulo define a classe `QueriesMixin`, responsável por fornecer consultas
SQL utilizadas pela UI e cálculos de saldos em `contas_a_pagar_mov`.

Funcionalidades principais
--------------------------
- Listar obrigações em aberto (`vw_cap_em_aberto`).
- Obter saldo em aberto de uma obrigação (`vw_cap_saldos`).
- Listar boletos em aberto com detalhamento de status e saldo calculado.

Detalhes técnicos
-----------------
- Usa `pandas.read_sql` para retornar DataFrames prontos para UI.
- Views `vw_cap_em_aberto` e `vw_cap_saldos` são usadas como fonte de dados.
- Para boletos, o saldo é recalculado diretamente da tabela `contas_a_pagar_mov`
  considerando LANCAMENTO, PAGAMENTO, MULTA, JUROS, DESCONTO e AJUSTE.
- Este mixin é combinado com `BaseRepo` na classe final (`ContasAPagarMovRepository`).

Dependências
------------
- pandas
"""

from __future__ import annotations

from typing import Any, Optional
import pandas as pd


class QueriesMixin(object):
    """Mixin de consultas para UI e cálculos de saldo."""

    def __init__(self, *args, **kwargs):
        # __init__ cooperativo para múltipla herança
        super().__init__(*args, **kwargs)

    # ---------------------------------------------------------------------
    # Helper de conexão (igual aos outros mixins)
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
        return self._get_conn()  # type: ignore[attr-defined]

    # ---------------------------------------------------------------------
    # Consultas
    # ---------------------------------------------------------------------
    def listar_em_aberto(self, conn: Any = None, tipo_obrigacao: Optional[str] = None) -> pd.DataFrame:
        """
        Retorna obrigações em aberto a partir de `vw_cap_em_aberto`.

        Parâmetros
        ----------
        tipo_obrigacao : 'BOLETO' | 'FATURA_CARTAO' | 'EMPRESTIMO' | None

        Retorno
        -------
        pd.DataFrame
            Colunas:
            obrigacao_id, tipo_obrigacao, credor, descricao, competencia,
            vencimento, total_lancado, total_pago, saldo_aberto, perc_quitado.
        """
        if tipo_obrigacao:
            sql = """
                SELECT obrigacao_id, tipo_obrigacao, credor, descricao, competencia, vencimento,
                       total_lancado, total_pago, saldo_aberto, perc_quitado
                FROM vw_cap_em_aberto
                WHERE tipo_obrigacao = ?
                ORDER BY date(vencimento) ASC, obrigacao_id ASC;
            """
            with self._conn_ctx(conn) as c:
                return pd.read_sql(sql, c, params=(tipo_obrigacao,))
        else:
            sql = """
                SELECT obrigacao_id, tipo_obrigacao, credor, descricao, competencia, vencimento,
                       total_lancado, total_pago, saldo_aberto, perc_quitado
                FROM vw_cap_em_aberto
                ORDER BY date(vencimento) ASC, tipo_obrigacao, obrigacao_id ASC;
            """
            with self._conn_ctx(conn) as c:
                return pd.read_sql(sql, c)

    def obter_saldo_obrigacao(self, conn: Any = None, obrigacao_id: int = 0) -> float:
        """
        Retorna o saldo em aberto (ou 0) de uma obrigação a partir de `vw_cap_saldos`.

        Parâmetros
        ----------
        obrigacao_id : int
            ID da obrigação.
        """
        with self._conn_ctx(conn) as c:
            row = c.execute(
                "SELECT COALESCE(saldo_aberto,0) FROM vw_cap_saldos WHERE obrigacao_id=?;",
                (int(obrigacao_id),),
            ).fetchone()
            return float(row[0]) if row else 0.0

    def listar_boletos_em_aberto_detalhado(self, conn: Any = None, credor: Optional[str] = None) -> pd.DataFrame:
        """
        Lista parcelas (LANCAMENTOS) de BOLETO em aberto ou parcial.

        Recalcula o saldo diretamente a partir de `contas_a_pagar_mov`,
        somando lançamentos, pagamentos e ajustes.

        Parâmetros
        ----------
        credor : str | None
            Se informado, filtra pelo credor.

        Retorno
        -------
        pd.DataFrame
            Colunas:
            obrigacao_id, credor, descricao, parcela_num, parcelas_total,
            vencimento, valor_parcela, saldo, status.
        """
        base_sql = """
            WITH base AS (
              SELECT
                cap.obrigacao_id,
                MIN(COALESCE(cap.credor, '')) AS credor,
                MIN(COALESCE(cap.descricao, '')) AS descricao,
                MIN(cap.parcela_num) AS parcela_num,
                MIN(cap.parcelas_total) AS parcelas_total,
                MIN(cap.vencimento) AS vencimento,
                SUM(CASE WHEN cap.categoria_evento='LANCAMENTO' THEN cap.valor_evento ELSE 0 END) AS total_lancado,
                SUM(CASE WHEN cap.categoria_evento='PAGAMENTO'  THEN -cap.valor_evento ELSE 0 END) AS total_pago,
                SUM(CASE 
                      WHEN cap.categoria_evento='MULTA'    THEN cap.valor_evento
                      WHEN cap.categoria_evento='JUROS'    THEN cap.valor_evento
                      WHEN cap.categoria_evento='DESCONTO' THEN cap.valor_evento
                      WHEN cap.categoria_evento='AJUSTE'   THEN cap.valor_evento
                      ELSE 0 END
                ) AS total_ajustes
              FROM contas_a_pagar_mov cap
              WHERE cap.tipo_obrigacao='BOLETO'
              GROUP BY cap.obrigacao_id
            )
            SELECT
              obrigacao_id,
              credor,
              descricao,
              parcela_num,
              parcelas_total,
              vencimento,
              ROUND(total_lancado, 2) AS valor_parcela,
              ROUND(total_lancado + COALESCE(total_ajustes,0) - COALESCE(total_pago,0), 2) AS saldo,
              CASE
                WHEN (total_lancado + COALESCE(total_ajustes,0) - COALESCE(total_pago,0)) <= 0.00001 THEN 'Quitado'
                WHEN COALESCE(total_pago,0) > 0 THEN 'Parcial'
                ELSE 'Em aberto'
              END AS status
            FROM base
            WHERE (total_lancado + COALESCE(total_ajustes,0) - COALESCE(total_pago,0)) > 0.00001
            {filtro_credor}
            ORDER BY DATE(COALESCE(vencimento, DATE('now'))) ASC, parcela_num ASC, obrigacao_id ASC;
        """
        sql = base_sql.format(
            filtro_credor="AND LOWER(TRIM(credor)) = LOWER(TRIM(?))" if credor else ""
        )
        with self._conn_ctx(conn) as c:
            if credor:
                return pd.read_sql(sql, c, params=(credor,))
            else:
                return pd.read_sql(sql, c)


# API pública explícita
__all__ = ["QueriesMixin"]
