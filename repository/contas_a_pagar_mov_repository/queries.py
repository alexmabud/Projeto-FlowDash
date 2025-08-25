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
- ESTE MIXIN **NÃO** herda de `BaseRepo`. Ele é combinado com `BaseRepo` na
  classe final (`ContasAPagarMovRepository`).

Dependências
------------
- pandas
"""

import pandas as pd


class QueriesMixin(object):
    """Mixin de consultas para UI e cálculos de saldo."""

    def __init__(self, *args, **kwargs):
        # __init__ cooperativo para múltipla herança
        super().__init__(*args, **kwargs)

    def listar_em_aberto(self, conn, tipo_obrigacao: str | None = None) -> pd.DataFrame:
        """
        Retorna obrigações em aberto a partir de `vw_cap_em_aberto`.

        Parâmetros
        ----------
        tipo_obrigacao : str | None
            Pode ser 'BOLETO', 'FATURA_CARTAO' ou 'EMPRESTIMO'.
            Se None, retorna todos os tipos.

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
        """
        Retorna o saldo em aberto (ou 0) de uma obrigação a partir de `vw_cap_saldos`.

        Parâmetros
        ----------
        obrigacao_id : int
            ID da obrigação.

        Retorno
        -------
        float
            Valor do saldo em aberto.
        """
        row = conn.execute(
            "SELECT COALESCE(saldo_aberto,0) FROM vw_cap_saldos WHERE obrigacao_id=?;",
            (obrigacao_id,),
        ).fetchone()
        return float(row[0]) if row else 0.0

    def listar_boletos_em_aberto_detalhado(self, conn, credor: str | None = None) -> pd.DataFrame:
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
        if credor:
            return pd.read_sql(sql, conn, params=(credor,))
        else:
            return pd.read_sql(sql, conn)


# API pública explícita
__all__ = ["QueriesMixin"]
