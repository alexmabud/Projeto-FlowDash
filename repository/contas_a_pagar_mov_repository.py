# repository/contas_a_pagar_mov_repository.py
"""
Repository: Contas a Pagar (CAP)

Modelo (Lei do Sistema)
-----------------------
- valor_evento .................. principal contratado da parcela.
- principal_pago_acumulado ...... soma do PRINCIPAL amortizado (inclui desconto).
- juros_pago_acumulado .......... soma dos JUROS pagos.
- multa_paga_acumulada .......... soma das MULTAS pagas.
- desconto_aplicado_acumulado ... soma dos DESCONTOS aplicados (desconto NÃO é caixa).
- valor_pago_acumulado .......... GASTO DE CAIXA acumulado = principal + juros + multa  (desconto não entra).
- status/faltante ............... dependem APENAS de principal_pago_acumulado.
- Não criamos linhas 'PAGAMENTO' no CAP (auditoria fica em movimentacoes_bancarias).
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

STATUS_ABERTO = "EM ABERTO"
STATUS_PARCIAL = "PARCIAL"
STATUS_QUITADO = "QUITADO"

_EPS = 0.005  # Tolerância para considerar “faltante > 0” em filtros/relatórios


class ContasAPagarMovRepository:
    """Repositório unificado de Contas a Pagar (CAP)."""

    def __init__(self, db_path: str) -> None:
        """Inicializa o repositório.

        Args:
            db_path: Caminho do arquivo SQLite.
        """
        self.db_path = db_path

    # ---------------------------------------------------------------------
    # Conexão
    # ---------------------------------------------------------------------
    @contextmanager
    def _conn_ctx(self, conn: Optional[sqlite3.Connection]) -> Iterator[sqlite3.Connection]:
        """Garante `row_factory=sqlite3.Row` para retornos dict-like.

        Se `conn` for fornecida, apenas ajusta/recupera o `row_factory` no escopo.
        Caso contrário, abre/commita/fecha a conexão automaticamente.

        Args:
            conn: Conexão SQLite existente (opcional).

        Yields:
            Conexão SQLite com `row_factory` configurado.
        """
        if conn is not None:
            old_rf = getattr(conn, "row_factory", None)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.row_factory = old_rf
        else:
            c = sqlite3.connect(self.db_path)
            c.row_factory = sqlite3.Row
            try:
                yield c
            finally:
                c.commit()
                c.close()

    def _get_row(self, cur: sqlite3.Cursor, row_id: int) -> Optional[sqlite3.Row]:
        """Busca linha por ID (tabela `contas_a_pagar_mov`)."""
        cur.execute("SELECT * FROM contas_a_pagar_mov WHERE id = ?", (int(row_id),))
        return cur.fetchone()

    def obter_por_id(self, conn: Optional[sqlite3.Connection], parcela_id: int) -> Optional[dict]:
        """Obtém um lançamento por ID.

        Args:
            conn: Conexão SQLite (opcional).
            parcela_id: ID do lançamento/parcela.

        Returns:
            Dicionário com as colunas do registro ou None se não encontrado.
        """
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            cur.execute("SELECT * FROM contas_a_pagar_mov WHERE id = ?", (int(parcela_id),))
            row = cur.fetchone()
            return dict(row) if row else None

    def proximo_obrigacao_id(self, conn: Optional[sqlite3.Connection] = None) -> int:
        """Calcula o próximo `obrigacao_id` disponível (MAX + 1)."""
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            cur.execute("SELECT COALESCE(MAX(obrigacao_id), 0) + 1 FROM contas_a_pagar_mov")
            return int(cur.fetchone()[0] or 1)

    # ---------------------------------------------------------------------
    # Criação de Lançamento
    # ---------------------------------------------------------------------
    def registrar_lancamento(
        self,
        conn: Optional[sqlite3.Connection],
        *,
        obrigacao_id: int,
        tipo_obrigacao: str,
        valor_total: float,
        data_evento: str,
        vencimento: Optional[str],
        descricao: Optional[str],
        credor: Optional[str],
        competencia: Optional[str],
        parcela_num: Optional[int],
        parcelas_total: Optional[int],
        usuario: str,
        tipo_origem: Optional[str] = None,
        cartao_id: Optional[int] = None,
        emprestimo_id: Optional[int] = None,
    ) -> int:
        """Insere um LANCAMENTO (principal a pagar) com acumuladores zerados.

        Define status inicial como 'EM ABERTO'.

        Args:
            obrigacao_id: Identificador da obrigação (agrupador de parcelas).
            tipo_obrigacao: BOLETO, FATURA_CARTAO, EMPRESTIMO, etc.
            valor_total: Principal da parcela.
            data_evento: Data do evento (YYYY-MM-DD).
            vencimento: Vencimento da parcela (YYYY-MM-DD).
            descricao: Descrição do lançamento.
            credor: Nome do credor.
            competencia: Competência (YYYY-MM).
            parcela_num: Número da parcela.
            parcelas_total: Total de parcelas na obrigação.
            usuario: Usuário operador.
            tipo_origem: Campo auxiliar de origem.
            cartao_id: Relacionamento com cartão (quando aplicável).
            emprestimo_id: Relacionamento com empréstimo (quando aplicável).

        Returns:
            ID do lançamento inserido.
        """
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            cur.execute(
                """
                INSERT INTO contas_a_pagar_mov
                    (obrigacao_id, tipo_obrigacao, categoria_evento, data_evento, vencimento,
                     valor_evento, descricao, credor, competencia, parcela_num, parcelas_total,
                     forma_pagamento, origem, ledger_id, usuario, created_at,
                     tipo_origem, cartao_id, emprestimo_id, status,
                     valor_pago_acumulado,                -- CAIXA gasto
                     juros_pago_acumulado, multa_paga_acumulada, desconto_aplicado_acumulado,
                     valor, data_pagamento,
                     principal_pago_acumulado)
                VALUES (?, ?, 'LANCAMENTO', ?, ?, ?, ?, ?, ?, ?, ?,
                        NULL, NULL, NULL, ?, datetime('now','localtime'),
                        ?, ?, ?, ?,
                        0, 0, 0, 0,
                        NULL, NULL,
                        0)
                """,
                (
                    int(obrigacao_id),
                    str(tipo_obrigacao),
                    str(data_evento),
                    (str(vencimento) if vencimento else None),
                    float(valor_total),
                    (descricao or None),
                    (credor or None),
                    (competencia or None),
                    (int(parcela_num) if parcela_num else None),
                    (int(parcelas_total) if parcelas_total else None),
                    str(usuario),
                    (tipo_origem or None),
                    (int(cartao_id) if cartao_id is not None else None),
                    (int(emprestimo_id) if emprestimo_id is not None else None),
                    STATUS_ABERTO,
                ),
            )
            return int(cur.lastrowid)

    # (LEGADO) Mantido para reprocessos antigos – NÃO usado na lógica atual.
    def registrar_pagamento(
        self,
        conn: Optional[sqlite3.Connection],
        *,
        obrigacao_id: int,
        tipo_obrigacao: str,
        valor_pago: float,
        data_evento: str,
        forma_pagamento: Optional[str],
        origem: Optional[str],
        ledger_id: Optional[int],
        usuario: str,
        descricao_extra: Optional[str] = None,
        trans_uid: Optional[str] = None,
    ) -> int:
        """Insere um evento 'PAGAMENTO' legado (sem efeito na regra atual)."""
        saida = -abs(float(valor_pago))
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            cur.execute(
                """
                INSERT INTO contas_a_pagar_mov
                    (obrigacao_id, tipo_obrigacao, categoria_evento, data_evento, vencimento,
                     valor_evento, descricao, credor, competencia, parcela_num, parcelas_total,
                     forma_pagamento, origem, ledger_id, usuario, created_at,
                     tipo_origem, cartao_id, emprestimo_id, status,
                     valor_pago_acumulado,
                     juros_pago_acumulado, multa_paga_acumulada, desconto_aplicado_acumulado,
                     principal_pago_acumulado)
                VALUES (?, ?, 'PAGAMENTO', ?, NULL,
                        ?, ?, NULL, NULL, NULL, NULL,
                        ?, ?, ?, ?, datetime('now','localtime'),
                        NULL, NULL, NULL, NULL,
                        0, 0, 0, 0)
                """,
                (
                    int(obrigacao_id),
                    str(tipo_obrigacao),
                    str(data_evento),
                    float(saida),
                    (descricao_extra or None),
                    (forma_pagamento or None),
                    (origem or None),
                    (int(ledger_id) if ledger_id is not None else None),
                    str(usuario),
                ),
            )
            return int(cur.lastrowid)

    # ---------------------------------------------------------------------
    # Pagamentos (PARCIAL e QUITAÇÃO TOTAL)
    # ---------------------------------------------------------------------
    def aplicar_pagamento_parcela(
        self,
        conn: Optional[sqlite3.Connection],
        payload: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Aplica pagamento **parcial** na PARCELA (padrão novo).

        Regras aplicadas:
        - principal_pago_acumulado += principal_aplicado + desconto_efetivo
        - juros/multa acumulam nos respectivos *_acumulado/a
        - desconto_aplicado_acumulado += desconto_efetivo
        - valor_pago_acumulado (CAIXA) += principal_aplicado + juros + multa
          (desconto NÃO entra no caixa)
        - status depende SOMENTE de principal_pago_acumulado vs valor_evento

        NOTA: não cria linha 'PAGAMENTO' (auditoria é em movimentacoes_bancarias).

        Args:
            conn: Conexão SQLite (opcional).
            payload/kwargs: Campos esperados:
                parcela_id (int), valor_base (float),
                juros (float), multa (float), desconto (float),
                data_evento (YYYY-MM-DD), usuario (str)

        Returns:
            Snapshot do efeito na parcela (dicionário com campos utilizados pelos serviços).
        """
        d = dict(payload or {})
        d.update(kwargs or {})

        parcela_id = int(d.get("parcela_id"))
        principal_in = float(d.get("valor_base", d.get("valor_pago", d.get("valor", 0.0))) or 0.0)
        juros_in = float(d.get("juros", 0.0) or 0.0)
        multa_in = float(d.get("multa", 0.0) or 0.0)
        desc_in = float(d.get("desconto", 0.0) or 0.0)

        data_evt = str(d.get("data_evento", d.get("data_pagamento", datetime.now().strftime("%Y-%m-%d"))))
        _ = d.get("usuario", "-")  # compat

        with self._conn_ctx(conn) as c:
            cur = c.cursor()

            row = self._get_row(cur, parcela_id)
            if not row:
                raise ValueError(f"Parcela id={parcela_id} não encontrada.")

            obrigacao_id = int(row["obrigacao_id"])
            valor_evento = float(row["valor_evento"] or 0.0)

            principal_atual = float(row["principal_pago_acumulado"] or 0.0)
            juros_atual = float(row["juros_pago_acumulado"] or 0.0)
            multa_atual = float(row["multa_paga_acumulada"] or 0.0)
            desc_atual = float(row["desconto_aplicado_acumulado"] or 0.0)
            caixa_atual = float(row["valor_pago_acumulado"] or 0.0)

            # faltante de principal (independe de desconto acumulado)
            faltante = max(0.0, round(valor_evento - principal_atual, 2))

            principal_aplicado = min(max(0.0, principal_in), faltante)
            faltante_pos = max(0.0, round(faltante - principal_aplicado, 2))
            desconto_efetivo = min(max(0.0, desc_in), faltante_pos)

            juros_aplicado = max(0.0, juros_in)
            multa_aplicada = max(0.0, multa_in)

            # caixa deste evento: principal + juros + multa (desconto não é caixa)
            saida_evento = round(principal_aplicado + juros_aplicado + multa_aplicada, 2)

            novo_principal = round(principal_atual + principal_aplicado + desconto_efetivo, 2)
            novo_juros = round(juros_atual + juros_aplicado, 2)
            novo_multa = round(multa_atual + multa_aplicada, 2)
            novo_desc = round(desc_atual + desconto_efetivo, 2)
            novo_caixa = round(caixa_atual + saida_evento, 2)

            if novo_principal + 1e-9 >= valor_evento:
                novo_status = STATUS_QUITADO
            elif novo_principal > 0:
                novo_status = STATUS_PARCIAL
            else:
                novo_status = STATUS_ABERTO

            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                SET principal_pago_acumulado     = ?,
                    juros_pago_acumulado         = ?,
                    multa_paga_acumulada         = ?,
                    desconto_aplicado_acumulado  = ?,
                    valor_pago_acumulado         = ?,
                    status                       = ?,
                    data_pagamento               = ?
                WHERE id = ?
                """,
                (
                    novo_principal,
                    novo_juros,
                    novo_multa,
                    novo_desc,
                    novo_caixa,
                    novo_status,
                    data_evt,
                    parcela_id,
                ),
            )

            restante_depois = max(0.0, round(valor_evento - novo_principal, 2))
            return {
                "parcela_id": parcela_id,
                "obrigacao_id": obrigacao_id,
                "principal_aplicado": float(principal_aplicado),
                "juros_aplicado": float(juros_aplicado),
                "multa_aplicada": float(multa_aplicada),
                "desconto_aplicado": float(desconto_efetivo),
                "saida_total": float(saida_evento),
                "valor_evento": float(valor_evento),
                "valor_pago_acumulado": float(novo_caixa),
                "restante": float(restante_depois),
                "status": novo_status,
                "data_pagamento": data_evt,
                "id_evento_cap": -1,  # sem linha 'PAGAMENTO'
            }

    def aplicar_pagamento_parcela_quitacao_total(
        self,
        conn: Optional[sqlite3.Connection],
        *,
        parcela_id: int,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        data_evento: str,
        forma_pagamento: Optional[str],
        origem: Optional[str],
        ledger_id: Optional[int],
        usuario: str,
    ) -> Dict[str, Any]:
        """Quita a parcela inteira com as novas regras.

        Regras:
            - `desconto` amortiza principal (até o faltante).
            - caixa = principal_aplicado + juros + multa  (desconto NÃO entra).

        Observação:
            Parâmetros `forma_pagamento`, `origem`, `ledger_id` são mantidos por compatibilidade,
            mas não alteram a regra de cálculo.

        Returns:
            Snapshot do efeito da quitação (dicionário).
        """
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            row = self._get_row(cur, int(parcela_id))
            if not row:
                raise ValueError(f"Parcela id={parcela_id} não encontrada.")

            obrigacao_id = int(row["obrigacao_id"])
            valor_evento = float(row["valor_evento"] or 0.0)

            principal_atual = float(row["principal_pago_acumulado"] or 0.0)
            juros_atual = float(row["juros_pago_acumulado"] or 0.0)
            multa_atual = float(row["multa_paga_acumulada"] or 0.0)
            desc_atual = float(row["desconto_aplicado_acumulado"] or 0.0)
            caixa_atual = float(row["valor_pago_acumulado"] or 0.0)

            faltante = max(0.0, round(valor_evento - principal_atual, 2))
            desconto_efetivo = min(max(0.0, float(desconto or 0.0)), faltante)
            principal_aplicado = max(0.0, round(faltante - desconto_efetivo, 2))

            juros_aplicado = max(0.0, float(juros or 0.0))
            multa_aplicada = max(0.0, float(multa or 0.0))

            saida_evento = round(principal_aplicado + juros_aplicado + multa_aplicada, 2)

            novo_principal = round(principal_atual + principal_aplicado + desconto_efetivo, 2)
            if novo_principal > valor_evento:
                novo_principal = valor_evento

            novo_juros = round(juros_atual + juros_aplicado, 2)
            novo_multa = round(multa_atual + multa_aplicada, 2)
            novo_desc = round(desc_atual + desconto_efetivo, 2)
            novo_caixa = round(caixa_atual + saida_evento, 2)

            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET principal_pago_acumulado     = ?,
                       juros_pago_acumulado         = ?,
                       multa_paga_acumulada         = ?,
                       desconto_aplicado_acumulado  = ?,
                       valor_pago_acumulado         = ?,  -- CAIXA
                       status                       = ?,
                       data_pagamento               = ?
                 WHERE id = ?
                """,
                (
                    novo_principal,
                    novo_juros,
                    novo_multa,
                    novo_desc,
                    novo_caixa,
                    STATUS_QUITADO,
                    str(data_evento),
                    int(parcela_id),
                ),
            )

            return {
                "parcela_id": int(parcela_id),
                "obrigacao_id": obrigacao_id,
                "principal_aplicado": float(principal_aplicado),
                "juros_aplicado": float(juros_aplicado),
                "multa_aplicada": float(multa_aplicada),
                "desconto_aplicado": float(desconto_efetivo),
                "saida_total": float(saida_evento),
                "valor_evento": float(valor_evento),
                "principal_pago_acumulado": float(novo_principal),
                "restante": 0.0,
                "status": STATUS_QUITADO,
                "data_pagamento": str(data_evento),
                "id_evento_cap": -1,
            }

    # ---------------------------------------------------------------------
    # Helper de baixo nível (atualiza UMA parcela com deltas prontos)
    # ---------------------------------------------------------------------
    def aplicar_rateio_parcela(
        self,
        conn: Optional[sqlite3.Connection],
        parcela_id: int,
        *,
        principal_delta: float = 0.0,
        juros_delta: float = 0.0,
        multa_delta: float = 0.0,
        desconto_delta: float = 0.0,
        caixa_gasto_delta: Optional[float] = None,
        data_pagamento: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Atualiza acumuladores da parcela a partir de deltas (sem FIFO).

        Regras:
            - Se `caixa_gasto_delta` for None, usa (principal + juros + multa).
              (desconto não é caixa; informe explicitamente se precisar outra regra).
            - Clampa principal em [0, valor_evento] e recalcula status por principal.

        Returns:
            Snapshot atualizado da parcela (dict) ou dict mínimo com `parcela_id`.
        """
        if caixa_gasto_delta is None:
            caixa_gasto_delta = (principal_delta or 0.0) + (juros_delta or 0.0) + (multa_delta or 0.0)
        principal_delta = max(0.0, float(principal_delta or 0.0))
        juros_delta = max(0.0, float(juros_delta or 0.0))
        multa_delta = max(0.0, float(multa_delta or 0.0))
        desconto_delta = max(0.0, float(desconto_delta or 0.0))
        caixa_gasto_delta = float(caixa_gasto_delta or 0.0)
        data_pagamento = str(data_pagamento or datetime.now().strftime("%Y-%m-%d"))

        with self._conn_ctx(conn) as c:
            cur = c.cursor()

            row = self._get_row(cur, parcela_id)
            if not row:
                raise ValueError(f"Parcela {parcela_id} não encontrada")

            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET principal_pago_acumulado     = COALESCE(principal_pago_acumulado,0) + ?,
                       juros_pago_acumulado         = COALESCE(juros_pago_acumulado,0) + ?,
                       multa_paga_acumulada         = COALESCE(multa_paga_acumulada,0) + ?,
                       desconto_aplicado_acumulado  = COALESCE(desconto_aplicado_acumulado,0) + ?,
                       valor_pago_acumulado         = COALESCE(valor_pago_acumulado,0) + ?, -- CAIXA
                       data_pagamento               = ?
                 WHERE id = ?;
                """,
                (
                    principal_delta,
                    juros_delta,
                    multa_delta,
                    desconto_delta,
                    caixa_gasto_delta,
                    data_pagamento,
                    parcela_id,
                ),
            )

            # clamp e status por principal
            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET principal_pago_acumulado =
                         CASE
                           WHEN principal_pago_acumulado < 0 THEN 0
                           WHEN principal_pago_acumulado > valor_evento THEN valor_evento
                           ELSE principal_pago_acumulado
                         END
                 WHERE id = ?;
                """,
                (parcela_id,),
            )
            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET status = CASE
                                  WHEN COALESCE(principal_pago_acumulado,0) >= COALESCE(valor_evento,0) THEN 'QUITADO'
                                  WHEN COALESCE(principal_pago_acumulado,0) > 0 THEN 'PARCIAL'
                                  ELSE COALESCE(status,'EM ABERTO')
                                END
                 WHERE id = ?;
                """,
                (parcela_id,),
            )

            snap = self.obter_por_id(c, parcela_id)
            return snap or {"parcela_id": parcela_id}

    # ---------------------------------------------------------------------
    # Listagens (UI)
    # ---------------------------------------------------------------------
    def listar_faturas_cartao_abertas(self, conn: Optional[sqlite3.Connection] = None) -> List[dict]:
        """Lista faturas em aberto com faltante de PRINCIPAL > 0."""
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            rows = cur.execute(
                f"""
                SELECT
                    id                                   AS parcela_id,
                    obrigacao_id,
                    COALESCE(descricao, '')              AS descricao,
                    COALESCE(data_evento, '')            AS data_evento,
                    COALESCE(credor, '')                 AS credor,
                    COALESCE(competencia, '')            AS competencia,
                    COALESCE(valor_evento, 0.0)          AS valor_total,
                    COALESCE(principal_pago_acumulado,0) AS principal_pago_acumulado,
                    ROUND(
                        CASE
                          WHEN (COALESCE(valor_evento,0) - COALESCE(principal_pago_acumulado,0)) < 0
                          THEN 0
                          ELSE (COALESCE(valor_evento,0) - COALESCE(principal_pago_acumulado,0))
                        END, 2
                    ) AS saldo_restante,
                    COALESCE(status, 'EM ABERTO')        AS status
                FROM contas_a_pagar_mov
                WHERE categoria_evento = 'LANCAMENTO'
                  AND (tipo_obrigacao = 'FATURA_CARTAO' OR tipo_origem = 'FATURA_CARTAO')
                  AND (COALESCE(valor_evento,0) - COALESCE(principal_pago_acumulado,0)) > {_EPS}
                ORDER BY DATE(COALESCE(data_evento,'1970-01-01')) DESC, id DESC
                """
            ).fetchall()
            return [dict(r) for r in rows]

    def listar_boletos_em_aberto(self, conn: Optional[sqlite3.Connection] = None) -> List[dict]:
        """Lista boletos em aberto/parcial (faltante de PRINCIPAL)."""
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            rows = cur.execute(
                """
                SELECT
                    id                                   AS parcela_id,
                    COALESCE(obrigacao_id, 0)            AS obrigacao_id,
                    TRIM(COALESCE(credor, ''))           AS credor,
                    TRIM(COALESCE(descricao, ''))        AS descricao,
                    COALESCE(parcela_num, 1)             AS parcela_num,
                    COALESCE(parcelas_total, 1)          AS parcelas_total,
                    DATE(vencimento)                     AS vencimento,
                    COALESCE(valor_evento, 0.0)          AS valor_evento,
                    COALESCE(principal_pago_acumulado,0) AS principal_pago_acumulado,
                    ROUND(
                        CASE
                          WHEN (COALESCE(valor_evento,0) - COALESCE(principal_pago_acumulado,0)) < 0
                          THEN 0
                          ELSE (COALESCE(valor_evento,0) - COALESCE(principal_pago_acumulado,0))
                        END, 2
                    ) AS em_aberto,
                    COALESCE(status, 'EM ABERTO')         AS status
                FROM contas_a_pagar_mov
                WHERE categoria_evento = 'LANCAMENTO'
                  AND UPPER(COALESCE(status,'EM ABERTO')) IN ('EM ABERTO','PARCIAL')
                  AND UPPER(COALESCE(tipo_obrigacao,'')) = 'BOLETO'
                ORDER BY DATE(vencimento) ASC, parcela_num ASC, id ASC
                """
            ).fetchall()
            return [dict(r) for r in rows if float(r["em_aberto"]) > _EPS]

    def listar_emprestimos_em_aberto(self, conn: Optional[sqlite3.Connection] = None) -> List[dict]:
        """Lista empréstimos em aberto/parcial (faltante de PRINCIPAL)."""
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            rows = cur.execute(
                """
                SELECT
                    id                                   AS parcela_id,
                    COALESCE(obrigacao_id, 0)            AS obrigacao_id,
                    TRIM(COALESCE(credor, ''))           AS credor,
                    TRIM(COALESCE(descricao, ''))        AS descricao,
                    COALESCE(parcela_num, 1)             AS parcela_num,
                    COALESCE(parcelas_total, 1)          AS parcelas_total,
                    DATE(vencimento)                     AS vencimento,
                    COALESCE(valor_evento, 0.0)          AS valor_evento,
                    COALESCE(principal_pago_acumulado,0) AS principal_pago_acumulado,
                    ROUND(
                        CASE
                          WHEN (COALESCE(valor_evento,0) - COALESCE(principal_pago_acumulado,0)) < 0
                          THEN 0
                          ELSE (COALESCE(valor_evento,0) - COALESCE(principal_pago_acumulado,0))
                        END, 2
                    ) AS em_aberto,
                    COALESCE(status, 'EM ABERTO')         AS status
                FROM contas_a_pagar_mov
                WHERE categoria_evento = 'LANCAMENTO'
                  AND UPPER(COALESCE(status,'EM ABERTO')) IN ('EM ABERTO','PARCIAL')
                  AND UPPER(COALESCE(tipo_obrigacao,'')) = 'EMPRESTIMO'
                ORDER BY DATE(vencimento) ASC, parcela_num ASC, id ASC
                """
            ).fetchall()
            return [dict(r) for r in rows if float(r["em_aberto"]) > _EPS]

    def obter_em_aberto(
        self, conn: Optional[sqlite3.Connection], tipo_obrigacao: Optional[str] = None
    ) -> List[dict]:
        """Lista LANCAMENTOS em aberto/parcial (qualquer tipo).

        `em_aberto` representa o faltante de PRINCIPAL.
        """
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            filtro = ""
            params: List[Any] = []
            if tipo_obrigacao:
                filtro = "AND UPPER(tipo_obrigacao)=UPPER(?)"
                params.append(tipo_obrigacao)
            rows = cur.execute(
                f"""
                SELECT
                    id, obrigacao_id, tipo_obrigacao, credor, descricao, vencimento,
                    valor_evento,
                    COALESCE(principal_pago_acumulado,0) AS principal_pago_acumulado,
                    (COALESCE(valor_evento,0) - COALESCE(principal_pago_acumulado,0)) AS em_aberto,
                    COALESCE(status, 'EM ABERTO')        AS status
                FROM contas_a_pagar_mov
                WHERE categoria_evento = 'LANCAMENTO'
                  AND UPPER(COALESCE(status,'EM ABERTO')) IN ('EM ABERTO','PARCIAL')
                  {filtro}
                ORDER BY DATE(vencimento) ASC, id ASC
                """,
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    # ---------------------------------------------------------------------
    # FIFO (por vencimento) para rateios nos services
    # ---------------------------------------------------------------------
    def listar_parcelas_em_aberto_fifo(
        self,
        conn: Optional[sqlite3.Connection],
        obrigacao_id: int,
        limite: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Lista parcelas de uma obrigação com faltante de PRINCIPAL > 0 (FIFO).

        Ordena por vencimento e, em caso de empate, por id.

        Args:
            conn: Conexão SQLite (opcional).
            obrigacao_id: Identificador da obrigação.
            limite: Limite opcional de registros.

        Returns:
            Lista de dicionários com: parcela_id, vencimento, valor_evento,
            principal_pago_acumulado e principal_faltante.
        """
        sql = """
            SELECT
                id AS parcela_id,
                DATE(vencimento) AS vencimento,
                COALESCE(valor_evento,0)                AS valor_evento,
                COALESCE(principal_pago_acumulado,0)    AS principal_pago_acumulado,
                (COALESCE(valor_evento,0) - COALESCE(principal_pago_acumulado,0)) AS principal_faltante
            FROM contas_a_pagar_mov
            WHERE obrigacao_id = ?
              AND (COALESCE(valor_evento,0) - COALESCE(principal_pago_acumulado,0)) > 0
            ORDER BY DATE(vencimento), id
        """
        params: List[Any] = [obrigacao_id]
        if limite is not None and int(limite) > 0:
            sql += " LIMIT ?"
            params.append(int(limite))

        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            rows = cur.execute(sql, params).fetchall()
            out: List[Dict[str, Any]] = [dict(r) for r in rows]
            return out
