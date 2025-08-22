"""
Módulo CartoesRepository
========================

Este módulo define a classe `CartoesRepository`, responsável por gerenciar
a tabela `cartoes` no banco de dados SQLite. Ele centraliza operações de
cadastro, consulta e manutenção de cartões de crédito utilizados no sistema.

Funcionalidades principais
--------------------------
- Criação automática do schema da tabela `cartoes`.
- Cadastro de novos cartões (nome, banco associado, data de vencimento, dia de fechamento).
- Alteração e exclusão de cartões cadastrados.
- Consulta de cartões ativos para uso em lançamentos de crédito.
- Integração com o `LedgerService` para geração de faturas futuras e controle
  de obrigações vinculadas.

Detalhes técnicos
-----------------
- Conexão SQLite configurada em modo WAL, com busy_timeout e suporte a
  foreign keys.
- Cada cartão possui campos de configuração essenciais para cálculo de
  competência e fatura (fechamento e vencimento).
- Estrutura pensada para suportar múltiplos cartões em paralelo, vinculados
  a diferentes bancos.

Dependências
------------
- sqlite3
- pandas
- typing (Optional, List, Dict)

"""

import sqlite3
from typing import Optional, Tuple, List


class CartoesRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _validar_conf(self, vencimento_dia: int, dias_fechamento: int) -> None:
        # dia do vencimento: 1..31 (ajustaremos para último dia do mês quando usar)
        if not (1 <= int(vencimento_dia) <= 31):
            raise ValueError(f"vencimento inválido ({vencimento_dia}); use 1..31")
        # offset de fechamento em dias: use um teto seguro (0..28 costuma cobrir todos os casos)
        if not (0 <= int(dias_fechamento) <= 28):
            raise ValueError(f"fechamento inválido ({dias_fechamento}); use 0..28 (dias antes do vencimento)")

    def obter_por_nome(self, nome: str) -> Optional[Tuple[int, int]]:
        """
        Retorna (vencimento_dia, dias_fechamento) do cartão ou None se não existir.

        Observação:
        - Coluna 'vencimento' armazena o DIA de vencimento (1..31).
        - Coluna 'fechamento' armazena QUANTOS DIAS antes do vencimento a fatura fecha (0..28).
        """
        if not nome or not nome.strip():
            return None
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT vencimento, fechamento
                  FROM cartoes_credito
                 WHERE LOWER(TRIM(nome)) = LOWER(TRIM(?))
                 LIMIT 1
                """,
                (nome,)
            ).fetchone()
            if not row:
                return None

            vencimento_dia = int(row[0] if row[0] is not None else 0)
            dias_fechamento = int(row[1] if row[1] is not None else 0)
            self._validar_conf(vencimento_dia, dias_fechamento)
            return (vencimento_dia, dias_fechamento)

    def listar_nomes(self) -> List[str]:
        """Lista nomes de cartões cadastrados, ordenados alfabeticamente (case/trim-insensitive)."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT nome FROM cartoes_credito ORDER BY LOWER(TRIM(nome)) ASC"
            ).fetchall()
            return [r[0] for r in rows]


# ----------------------------
# FUNÇÃO EXTRA — fora da classe
# ----------------------------
def listar_destinos_fatura_em_aberto(db_path: str):
    """
    Retorna faturas em aberto (uma por cartão+mês), com o 'obrigacao_id' do LANCAMENTO.
    Formato:
    [
      {
        "label": "Fatura Cartão Bradesco 2025-08 — R$ 400,00",
        "cartao": "Cartão Bradesco",
        "competencia": "2025-08",
        "obrigacao_id": 123,
        "saldo": 400.0
      },
      ...
    ]
    """
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            WITH lanc AS (
                SELECT
                    obrigacao_id,
                    credor        AS cartao,
                    competencia,
                    COALESCE(valor_evento,0) AS total_lancado
                FROM contas_a_pagar_mov
                WHERE tipo_obrigacao='FATURA_CARTAO'
                  AND categoria_evento='LANCAMENTO'
                  AND COALESCE(credor,'') <> ''
                  AND COALESCE(competencia,'') <> ''
            ),
            pagos AS (
                SELECT
                    obrigacao_id,
                    COALESCE(SUM(-valor_evento),0) AS total_pago  -- pagamento é negativo
                FROM contas_a_pagar_mov
                WHERE UPPER(COALESCE(categoria_evento,'')) LIKE 'PAGAMENTO%'
                GROUP BY obrigacao_id
            )
            SELECT
                l.obrigacao_id,
                l.cartao,
                l.competencia,
                ROUND(l.total_lancado - COALESCE(p.total_pago,0), 2) AS saldo
            FROM lanc l
            LEFT JOIN pagos p USING (obrigacao_id)
            WHERE (l.total_lancado - COALESCE(p.total_pago,0)) > 0
            ORDER BY LOWER(TRIM(l.cartao)) ASC, l.competencia ASC
        """).fetchall()

    itens = []
    for r in rows:
        cartao = r["cartao"] or ""
        comp   = r["competencia"] or ""
        saldo  = float(r["saldo"] or 0.0)
        label  = f"Fatura {cartao} {comp} — R$ {saldo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        itens.append({
            "label": label,
            "cartao": cartao,
            "competencia": comp,
            "obrigacao_id": int(r["obrigacao_id"]),
            "saldo": saldo,
        })
    return itens