
"""Auto-baixa e classificação (_AutoBaixaLedgerMixin).

Implementa:
- Classificação de títulos por destino
- Pagamento direto por obrigação (fatura)
- Auto-baixa para faturas, boletos e empréstimos
"""
from __future__ import annotations
import sqlite3
import pandas as pd
from typing import Optional, List

class _AutoBaixaLedgerMixin:
    """Mixin com rotinas de classificação e auto-baixa de títulos."""

    # --- pagamento direto por OBRIGACAO (fatura) ----------------------------------------
    def _pagar_fatura_por_obrigacao(self, conn: sqlite3.Connection, *, obrigacao_id: int, valor: float,
                                    data_evento: str, forma_pagamento: str,
                                    origem: str, ledger_id: int, usuario: str) -> float:
        cur = conn.cursor()
        row = cur.execute("""
            SELECT id, COALESCE(valor_evento,0) AS valor_doc
              FROM contas_a_pagar_mov
             WHERE obrigacao_id = ?
               AND categoria_evento = 'LANCAMENTO'
               AND (tipo_obrigacao='FATURA_CARTAO' OR tipo_origem='FATURA_CARTAO')
             LIMIT 1
        """, (int(obrigacao_id),)).fetchone()
        if not row:
            raise ValueError(f"Fatura (obrigacao_id={obrigacao_id}) não encontrada.")

        lanc_id = int(row[0])
        valor_doc = float(row[1])

        ja_pago = self._total_pago_acumulado(conn, int(obrigacao_id))
        falta = max(0.0, round(valor_doc - ja_pago, 2))
        if falta <= 0:
            self._atualizar_status_por_obrigacao(conn, int(obrigacao_id))
            return float(valor)

        pagar = min(float(valor), falta)

        self.cap_repo.registrar_pagamento(
            conn,
            obrigacao_id=int(obrigacao_id),
            tipo_obrigacao="FATURA_CARTAO",
            valor_pago=float(pagar),
            data_evento=data_evento,
            forma_pagamento=forma_pagamento,
            origem=origem,
            ledger_id=int(ledger_id),
            usuario=usuario,
        )

        self._atualizar_status_por_obrigacao(conn, int(obrigacao_id))
        sobra = round(float(valor) - pagar, 2)
        return sobra

    # --- classificação por destino ------------------------------------------------------
    def _classificar_conta_a_pagar_por_destino(self, conn: sqlite3.Connection, pagamento_tipo: Optional[str], pagamento_destino: Optional[str]) -> int:
        if not pagamento_tipo or not pagamento_destino or not str(pagamento_destino).strip():
            return 0

        destino = str(pagamento_destino).strip()
        cur = conn.cursor()

        if pagamento_tipo in ("Fatura Cartao de Credito", "Fatura Cartão de Crédito"):
            row = cur.execute("""
                SELECT id FROM cartoes_credito
                WHERE LOWER(TRIM(nome)) = LOWER(TRIM(?))
                LIMIT 1
            """, (destino,)).fetchone()
            cartao_id = int(row[0]) if row else None

            if cartao_id is not None:
                cur.execute("""
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='FATURA_CARTAO', cartao_id=?
                     WHERE LOWER(TRIM(credor)) = LOWER(TRIM(?))
                """, (cartao_id, destino))
            else:
                cur.execute("""
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='FATURA_CARTAO', cartao_id=NULL
                     WHERE LOWER(TRIM(credor)) = LOWER(TRIM(?))
                """, (destino,))
            return cur.rowcount

        elif pagamento_tipo == "Boletos":
            cur.execute("""
                UPDATE contas_a_pagar_mov
                   SET tipo_origem='BOLETO', cartao_id=NULL, emprestimo_id=NULL
                 WHERE LOWER(TRIM(credor)) = LOWER(TRIM(?))
            """, (destino,))
            return cur.rowcount

        elif pagamento_tipo in ("Emprestimos e Financiamentos", "Empréstimos e Financiamentos"):
            row = cur.execute("""
                SELECT id
                  FROM emprestimos_financiamentos
                 WHERE LOWER(TRIM(COALESCE(NULLIF(banco,''), NULLIF(descricao,''), NULLIF(tipo,''))))
                       = LOWER(TRIM(?))
                 LIMIT 1
            """, (destino,)).fetchone()
            emp_id = int(row[0]) if row else None

            if emp_id is not None:
                cur.execute("""
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='EMPRESTIMO', emprestimo_id=?
                     WHERE LOWER(TRIM(credor)) = LOWER(TRIM(?))
                """, (emp_id, destino))
            else:
                cur.execute("""
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='EMPRESTIMO', emprestimo_id=NULL
                     WHERE LOWER(TRIM(credor)) = LOWER(TRIM(?))
                """, (destino,))
            return cur.rowcount

        return 0

    # --- auto-baixa (fatura/boletos/emprestimo) ----------------------------------------
    def _auto_baixar_pagamentos_emprestimo(
        self,
        conn: sqlite3.Connection,
        *,
        data: str,
        total_saida: float,
        forma_pagamento: str,
        origem: str,
        destino: str,
        usuario: str,
        ledger_id: int
    ) -> list[int]:
        resto = float(max(total_saida, 0.0))
        if resto <= 0 or not destino:
            return []

        df = pd.read_sql("""
            SELECT obrigacao_id, saldo_aberto, vencimento
              FROM vw_cap_em_aberto
             WHERE tipo_obrigacao = 'EMPRESTIMO'
               AND LOWER(TRIM(credor)) = LOWER(TRIM(?))
             ORDER BY DATE(vencimento) ASC, obrigacao_id ASC
        """, conn, params=(destino,))
        if df.empty:
            return []

        eventos_ids: List[int] = []
        for _, r in df.iterrows():
            if resto <= 0:
                break
            obrig_id = int(r["obrigacao_id"])
            saldo    = float(r["saldo_aberto"] or 0.0)
            if saldo <= 0:
                continue
            pagar = min(resto, saldo)

            ev_id = self.cap_repo.registrar_pagamento(
                conn,
                obrigacao_id=obrig_id,
                tipo_obrigacao="EMPRESTIMO",
                valor_pago=pagar,
                data_evento=data,
                forma_pagamento=forma_pagamento,
                origem=origem,
                ledger_id=int(ledger_id),
                usuario=usuario
            )
            eventos_ids.append(int(ev_id))
            resto -= pagar

        return eventos_ids

    def _auto_baixar_pagamentos(self, conn: sqlite3.Connection, *,
                                pagamento_tipo: str,
                                pagamento_destino: str,
                                valor_total: float,
                                data_evento: str,
                                forma_pagamento: str,
                                origem: str,
                                ledger_id: int,
                                usuario: str,
                                competencia_pagamento: str | None = None) -> float:
        restante = float(valor_total)
        if restante <= 0 or not pagamento_tipo or not (pagamento_destino or "").strip():
            return restante

        tipo_norm = (pagamento_tipo or "").strip().lower()

        # Emprestimos: usa helper dedicado
        if tipo_norm in ("emprestimos e financiamentos", "empréstimos e financiamentos"):
            try:
                eventos = self._auto_baixar_pagamentos_emprestimo(
                    conn,
                    data=data_evento,
                    total_saida=restante,
                    forma_pagamento=forma_pagamento,
                    origem=origem,
                    destino=pagamento_destino.strip(),
                    usuario=usuario,
                    ledger_id=int(ledger_id)
                )
                return 0.0 if eventos else restante
            except Exception:
                return restante

        # Faturas/Boletos
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        expr_valor_doc = self._expr_valor_documento(conn)

        aberto_where = (
            "COALESCE(status, 'Em aberto') = 'Em aberto' "
            "AND COALESCE(categoria_evento,'') = 'LANCAMENTO'"
        )

        if pagamento_tipo in ("Fatura Cartao de Credito", "Fatura Cartão de Crédito"):
            tipo_alvo = "FATURA_CARTAO"
            comp_sql = " AND competencia = ? " if competencia_pagamento else ""
            params_tail = ([competencia_pagamento] if competencia_pagamento else [])

            rows = cur.execute(f"""
                SELECT id, obrigacao_id,
                       {expr_valor_doc} AS valor_documento,
                       COALESCE(vencimento, data_evento) AS vcto
                  FROM contas_a_pagar_mov
                 WHERE (tipo_obrigacao = ? OR tipo_origem = ?)
                   AND {aberto_where}
                   AND LOWER(TRIM(credor)) = LOWER(TRIM(?))
                   {comp_sql}
                 ORDER BY DATE(vcto) ASC, id ASC
            """, (tipo_alvo, tipo_alvo, pagamento_destino, *params_tail)).fetchall()

        elif pagamento_tipo == "Boletos":
            tipo_alvo = "BOLETO"
            rows = cur.execute(f"""
                SELECT id, obrigacao_id,
                       {expr_valor_doc} AS valor_documento,
                       COALESCE(vencimento, data_evento) AS vcto
                  FROM contas_a_pagar_mov
                 WHERE (tipo_obrigacao = ? OR tipo_origem = ?)
                   AND {aberto_where}
                   AND LOWER(TRIM(credor)) = LOWER(TRIM(?))
                 ORDER BY DATE(vcto) ASC, id ASC
            """, (tipo_alvo, tipo_alvo, pagamento_destino)).fetchall()

        else:
            return restante

        if not rows:
            if competencia_pagamento and pagamento_tipo in ("Fatura Cartao de Credito", "Fatura Cartão de Crédito"):
                raise ValueError(
                    f"Nenhuma fatura em aberto encontrada para '{pagamento_destino}' em {competencia_pagamento}."
                )
            return restante

        for row in rows:
            if restante <= 0:
                break

            row_id = int(row["id"])
            obrigacao_id = int(row["obrigacao_id"])
            valor_doc = float(row["valor_documento"] or 0.0)
            if valor_doc <= 0:
                continue

            ja_pago = self._total_pago_acumulado(conn, obrigacao_id)
            falta = max(0.0, round(valor_doc - ja_pago, 2))
            if falta <= 0:
                self._atualizar_status_por_id(conn, row_id, obrigacao_id, valor_doc)
                continue

            pagar = min(restante, falta)

            self.cap_repo.registrar_pagamento(
                conn,
                obrigacao_id=obrigacao_id,
                tipo_obrigacao=tipo_alvo,
                valor_pago=float(pagar),
                data_evento=data_evento,
                forma_pagamento=forma_pagamento,
                origem=origem,
                ledger_id=int(ledger_id),
                usuario=usuario,
            )

            self._atualizar_status_por_id(conn, row_id, obrigacao_id, valor_doc)
            restante = round(restante - pagar, 2)

            if competencia_pagamento:
                break

        return restante
