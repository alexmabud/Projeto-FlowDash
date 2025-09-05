"""
Módulo Cartões (Repositório)
============================

Este módulo define a classe `CartoesRepository`, responsável por acessar e
gerenciar a tabela **`cartoes_credito`** no SQLite. Centraliza operações de
consulta e validação de configuração dos cartões usados em lançamentos de
crédito (vencimento e fechamento).

Funcionalidades principais
--------------------------
- Validação de configuração de cartão (dia de vencimento e dias de fechamento).
- Consulta de cartão por nome → retorna `(vencimento_dia, dias_fechamento)` ou dict.
- Listagem de nomes de cartões (ordenada e normalizada).

Detalhes técnicos
-----------------
- Conexão SQLite configurada com:
  - `PRAGMA journal_mode=WAL;`
  - `PRAGMA busy_timeout=30000;`
  - `PRAGMA foreign_keys=ON;`
- Comparações de nome **case/trim-insensitive** no SQL.
- Não altera schema nem dados; foco em leitura e validação.

Dependências
------------
- sqlite3
- typing (Optional, Tuple, List, Dict)
"""

from __future__ import annotations

import sqlite3
from typing import Optional, Tuple, List, Dict


class CartoesRepository:
    """
    Repositório para operações de leitura/validação sobre `cartoes_credito`.

    Parâmetros:
        db_path (str): Caminho do arquivo SQLite.
    """
    def __init__(self, db_path: str):
        if not db_path or not isinstance(db_path, str):
            raise ValueError("db_path inválido para CartoesRepository")
        self.db_path = db_path

    # -------------------------
    # Conexão com PRAGMAs
    # -------------------------
    def _get_conn(self) -> sqlite3.Connection:
        """
        Abre conexão SQLite com PRAGMAs de confiabilidade/performance
        adequados ao app (WAL, busy_timeout, foreign_keys).
        """
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    # -------------------------
    # Validações de configuração
    # -------------------------
    def _validar_conf(self, vencimento_dia: int, dias_fechamento: int) -> None:
        """
        Valida parâmetros de configuração de fatura:

        - `vencimento_dia`: 1..31 (ajuste para último dia do mês ocorre no uso).
        - `dias_fechamento`: 0..28 (dias ANTES do vencimento em que fecha).
        """
        try:
            v = int(vencimento_dia)
            f = int(dias_fechamento)
        except Exception:
            raise ValueError("vencimento/dias_fechamento precisam ser inteiros")

        if not (1 <= v <= 31):
            raise ValueError(f"vencimento inválido ({vencimento_dia}); use 1..31")
        if not (0 <= f <= 28):
            raise ValueError(f"fechamento inválido ({dias_fechamento}); use 0..28 (dias antes do vencimento)")

    # -------------------------
    # Consultas
    # -------------------------
    def obter_por_nome(self, nome: str) -> Optional[Tuple[int, int]]:
        """
        Retorna `(vencimento_dia, dias_fechamento)` do cartão, ou `None` se não existir.

        Notas:
            - `vencimento` guarda o **dia** do vencimento (1..31).
            - `fechamento` guarda **quantos dias antes** do vencimento a fatura fecha (0..28).

        Parâmetros:
            nome (str): Nome do cartão (case/trim-insensitive).

        Retorno:
            Optional[Tuple[int, int]]: `(vencimento_dia, dias_fechamento)` ou None.
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

    # Aliases comuns para compatibilidade
    def get_by_name(self, nome: str) -> Optional[Tuple[int, int]]:
        """Alias para `obter_por_nome` (compat)."""
        return self.obter_por_nome(nome)

    def get_config_by_name(self, nome: str) -> Optional[Dict[str, int]]:
        """
        Variante que retorna dict:
        { 'vencimento_dia': int, 'dias_fechamento': int } ou None
        """
        t = self.obter_por_nome(nome)
        if not t:
            return None
        v, f = t
        return {"vencimento_dia": v, "dias_fechamento": f}

    def exists(self, nome: str) -> bool:
        """Retorna True se o cartão existir, comparando por nome (case/trim-insensitive)."""
        if not nome or not nome.strip():
            return False
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT 1
                  FROM cartoes_credito
                 WHERE LOWER(TRIM(nome)) = LOWER(TRIM(?))
                 LIMIT 1
                """,
                (nome,)
            ).fetchone()
        return bool(row)

    def listar_nomes(self) -> List[str]:
        """
        Lista nomes de cartões cadastrados, ordenados alfabeticamente
        (case/trim-insensitive).
        """
        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT nome FROM cartoes_credito WHERE COALESCE(TRIM(nome),'') <> '' ORDER BY LOWER(TRIM(nome)) ASC"
            ).fetchall()
        return [str(r["nome"]) for r in rows]

# ----------------------------
# FUNÇÃO EXTRA — fora da classe
# ----------------------------
def listar_destinos_fatura_em_aberto(db_path: str):
    """
    Consulta faturas de cartão **em aberto** (uma por cartão+competência),
    retornando rótulos prontos para selects de UI.

    Regra:
        - Base: `contas_a_pagar_mov`
        - Lançamento de fatura: `tipo_obrigacao='FATURA_CARTAO'` e `categoria_evento='LANCAMENTO'`
        - Pagamentos deduzidos: eventos onde `categoria_evento` começa com 'PAGAMENTO'
          (valores negativos somados para obter total pago).
        - `saldo = total_lancado - total_pago` (apenas `saldo > 0` entra no resultado).

    Retorno:
        list[dict]: Cada item contém:
            - label (str): Ex.: "Fatura Bradesco 2025-08 — R$ 400,00"
            - cartao (str)
            - competencia (str)  # YYYY-MM
            - obrigacao_id (int)
            - saldo (float)
    """
    if not db_path or not isinstance(db_path, str):
        raise ValueError("db_path inválido em listar_destinos_fatura_em_aberto")

    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.row_factory = sqlite3.Row

        rows = conn.execute(
            """
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
            """
        ).fetchall()
    finally:
        conn.close()

    itens = []
    for r in rows:
        cartao = r["cartao"] or ""
        comp = r["competencia"] or ""
        saldo = float(r["saldo"] or 0.0)
        # BRL simples (R$ 1.234,56)
        label = f"Fatura {cartao} {comp} — R$ {saldo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        itens.append(
            {
                "label": label,
                "cartao": cartao,
                "competencia": comp,
                "obrigacao_id": int(r["obrigacao_id"]),
                "saldo": saldo,
            }
        )
    return itens


# API pública explícita
__all__ = ["CartoesRepository", "listar_destinos_fatura_em_aberto"]
