"""
Consultas para a UI e cálculos de saldo.
"""

import pandas as pd
from .base import BaseRepo


class QueriesMixin(BaseRepo):
    def listar_em_aberto(self, conn, tipo_obrigacao: str | None = None) -> pd.DataFrame:
        """
        Retorna obrigações em aberto a partir de vw_cap_em_aberto.
        Se tipo_obrigacao vier ('BOLETO'|'FATURA_CARTAO'|'EMPRESTIMO'), filtra por tipo.
        """
        if tipo_obrigacao:
            sql = """
                SELECT obrigacao_id, tipo_obrigacao, credor, descricao, competencia, vencimento,
                       total_lancado, total_pago, saldo_aberto, perc_quitado
                FROM vw_cap_em_aberto
                WHERE tipo_obrigacao = ?
                ORDER BY date(vencimento) ASC NULLS LAST, obrigacao_id ASC;
            """
            return pd.read_sql(sql, conn, params=(tipo_obrigacao,))
        else:
            sql = """
                SELECT obrigacao_id, tipo_obrigacao, credor, descricao, competencia, vencimento,
                       total_lancado, total_pago, saldo_aberto, perc_quitado
                FROM vw_cap_em_aberto
                ORDER BY date(vencimento) ASC NULLS LAST, tipo_obrigacao, obrigacao_id ASC;
            """
            return pd.read_sql(sql, conn)

    def obter_saldo_obrigacao(self, conn, obrigacao_id: int) -> float:
        """Retorna o saldo em aberto (ou 0) a partir de vw_cap_saldos."""
        row = conn.execute(
            "SELECT COALESCE(saldo_aberto,0) FROM vw_cap_saldos WHERE obrigacao_id=?;",
            (obrigacao_id,)
        ).fetchone()
        return float(row[0]) if row else 0.0

    def listar_boletos_em_aberto_detalhado(self, conn, credor: str | None = None) -> pd.DataFrame:
        """
        Lista parcelas (LANCAMENTOS) de BOLETO em aberto/parcial, calculando saldo pela própria tabela de eventos.
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
                      WHEN cap.categoria_evento='DESCONTO' THEN cap.valor_evento  -- já negativo
                      WHEN cap.categoria_evento='AJUSTE'   THEN cap.valor_evento  -- legado (pode ser negativo)
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
        if credor:
            return pd.read_sql(sql, conn, params=(credor,))
        else:
            return pd.read_sql(sql, conn)
