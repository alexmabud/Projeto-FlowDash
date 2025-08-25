"""
service_ledger_autobaixa.py — Auto‑baixa e classificação de títulos.

Resumo:
    Rotinas de classificação por destino (cartão/boletos/emprestimos) e
    auto‑baixa de pagamentos em contas_a_pagar_mov (faturas, boletos, empréstimos).

Responsabilidades:
    - Classificar lançamentos por destino (cartões, boletos, empréstimos).
    - Pagar fatura diretamente por obrigacao_id.
    - Auto‑baixar pagamentos priorizando vencimentos mais antigos.

Depende de:
    - pandas.read_sql
    - sqlite3 (conexão gerenciada pelo chamador)
    - Repositório de CAP: self.cap_repo.registrar_pagamento(...)
    - Helpers do Ledger: _expr_valor_documento, _total_pago_acumulado,
      _atualizar_status_por_id, _atualizar_status_por_obrigacao

Efeitos colaterais:
    - Escreve/atualiza registros em contas_a_pagar_mov (tabelas/views correlatas).
    - Não faz commit: o controle transacional é do chamador.

Notas de segurança:
    - Nenhuma f-string em SQL com dados do usuário; apenas parâmetros (?).
    - A string retornada por _expr_valor_documento deve ser segura (constante).
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Optional, List

import pandas as pd

logger = logging.getLogger(__name__)

__all__ = ["_AutoBaixaLedgerMixin"]


class _AutoBaixaLedgerMixin:
    """Mixin com rotinas de classificação e auto‑baixa de títulos (CAP)."""

    # --- pagamento direto por OBRIGACAO (fatura) ----------------------------------------
    def _pagar_fatura_por_obrigacao(
        self,
        conn: sqlite3.Connection,
        *,
        obrigacao_id: int,
        valor: float,
        data_evento: str,
        forma_pagamento: str,
        origem: str,
        ledger_id: int,
        usuario: str,
    ) -> float:
        """
        Paga uma fatura (FATURA_CARTAO) vinculada à `obrigacao_id` até o limite de `valor`.

        Retorna:
            float: eventual sobra do valor informado, caso o pagamento não consuma tudo.
        """
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT id, COALESCE(valor_evento,0) AS valor_doc
              FROM contas_a_pagar_mov
             WHERE obrigacao_id = ?
               AND categoria_evento = 'LANCAMENTO'
               AND (tipo_obrigacao='FATURA_CARTAO' OR tipo_origem='FATURA_CARTAO')
             LIMIT 1
            """,
            (int(obrigacao_id),),
        ).fetchone()
        if not row:
            raise ValueError(f"Fatura (obrigacao_id={obrigacao_id}) não encontrada.")

        lanc_id = int(row[0])
        valor_doc = float(row[1])

        ja_pago = self._total_pago_acumulado(conn, int(obrigacao_id))
        falta = max(0.0, round(valor_doc - ja_pago, 2))
        if falta <= 0:
            # já liquidada: apenas garantir status
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
        logger.debug(
            "Pagou fatura por obrigacao_id=%s (lanc_id=%s): pagar=%.2f, sobra=%.2f",
            obrigacao_id, lanc_id, pagar, sobra
        )
        return sobra

    # --- classificação por destino ------------------------------------------------------
    def _classificar_conta_a_pagar_por_destino(
        self,
        conn: sqlite3.Connection,
        pagamento_tipo: Optional[str],
        pagamento_destino: Optional[str],
    ) -> int:
        """
        Classifica lançamentos de CAP conforme destino (cartão/boletos/emprestimos).

        Retorna:
            int: número de linhas atualizadas.
        """
        if not pagamento_tipo or not pagamento_destino or not str(pagamento_destino).strip():
            return 0

        destino = str(pagamento_destino).strip()
        cur = conn.cursor()

        if pagamento_tipo in ("Fatura Cartao de Credito", "Fatura Cartão de Crédito"):
            row = cur.execute(
                """
                SELECT id
                  FROM cartoes_credito
                 WHERE LOWER(TRIM(nome)) = LOWER(TRIM(?))
                 LIMIT 1
                """,
                (destino,),
            ).fetchone()
            cartao_id = int(row[0]) if row else None

            if cartao_id is not None:
                cur.execute(
                    """
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='FATURA_CARTAO', cartao_id=?
                     WHERE LOWER(TRIM(credor)) = LOWER(TRIM(?))
                    """,
                    (cartao_id, destino),
                )
            else:
                # ainda classifica como fatura mesmo sem match de cartão
                cur.execute(
                    """
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='FATURA_CARTAO', cartao_id=NULL
                     WHERE LOWER(TRIM(credor)) = LOWER(TRIM(?))
                    """,
                    (destino,),
                )
            return cur.rowcount

        elif pagamento_tipo == "Boletos":
            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET tipo_origem='BOLETO', cartao_id=NULL, emprestimo_id=NULL
                 WHERE LOWER(TRIM(credor)) = LOWER(TRIM(?))
                """,
                (destino,),
            )
            return cur.rowcount

        elif pagamento_tipo in ("Emprestimos e Financiamentos", "Empréstimos e Financiamentos"):
            row = cur.execute(
                """
                SELECT id
                  FROM emprestimos_financiamentos
                 WHERE LOWER(TRIM(COALESCE(NULLIF(banco,''), NULLIF(descricao,''), NULLIF(tipo,''))))
                       = LOWER(TRIM(?))
                 LIMIT 1
                """,
                (destino,),
            ).fetchone()
            emp_id = int(row[0]) if row else None

            if emp_id is not None:
                cur.execute(
                    """
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='EMPRESTIMO', emprestimo_id=?
                     WHERE LOWER(TRIM(credor)) = LOWER(TRIM(?))
                    """,
                    (emp_id, destino),
                )
            else:
                cur.execute(
                    """
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='EMPRESTIMO', emprestimo_id=NULL
                     WHERE LOWER(TRIM(credor)) = LOWER(TRIM(?))
                    """,
                    (destino,),
                )
            return cur.rowcount

        return 0

    # --- auto‑baixa (empréstimo) -------------------------------------------------------
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
        ledger_id: int,
    ) -> List[int]:
        """
        Auto‑baixa de parcelas de empréstimos para um `destino` (credor/banco/descrição),
        priorizando vencimentos mais antigos.

        Retorna:
            list[int]: IDs de eventos/pagamentos gerados (quando o repo expõe).
        """
        resto = float(max(total_saida, 0.0))
        if resto <= 0 or not destino:
            return []

        df = pd.read_sql(
            """
            SELECT obrigacao_id, saldo_aberto, vencimento
              FROM vw_cap_em_aberto
             WHERE tipo_obrigacao = 'EMPRESTIMO'
               AND LOWER(TRIM(credor)) = LOWER(TRIM(?))
             ORDER BY DATE(vencimento) ASC, obrigacao_id ASC
            """,
            conn,
            params=(destino,),
        )
        if df.empty:
            return []

        eventos_ids: List[int] = []
        for _, r in df.iterrows():
            if resto <= 0:
                break
            obrig_id = int(r["obrigacao_id"])
            saldo = float(r["saldo_aberto"] or 0.0)
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
                usuario=usuario,
            )
            try:
                eventos_ids.append(int(ev_id))
            except Exception:
                # alguns repositórios podem não retornar ID
                pass
            resto = round(resto - pagar, 2)

        logger.debug(
            "Auto-baixa empréstimo destino=%s total_saida=%.2f eventos=%s resto=%.2f",
            destino, total_saida, eventos_ids, resto
        )
        return eventos_ids

    # --- auto‑baixa (fatura/boletos) ---------------------------------------------------
    def _auto_baixar_pagamentos(
        self,
        conn: sqlite3.Connection,
        *,
        pagamento_tipo: str,
        pagamento_destino: str,
        valor_total: float,
        data_evento: str,
        forma_pagamento: str,
        origem: str,
        ledger_id: int,
        usuario: str,
        competencia_pagamento: Optional[str] = None,
    ) -> float:
        """
        Auto‑baixa para faturas/boletos (e delega a empréstimos).

        Retorna:
            float: valor restante (não consumido) após as baixas.
        """
        restante = float(valor_total)
        if restante <= 0 or not pagamento_tipo or not (pagamento_destino or "").strip():
            return restante

        tipo_norm = (pagamento_tipo or "").strip().lower()

        # Empréstimos: usa helper dedicado
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
                    ledger_id=int(ledger_id),
                )
                return 0.0 if eventos else restante
            except Exception as e:
                logger.exception("Falha na auto-baixa de empréstimos: %s", e)
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
            comp_sql = " AND competencia = ? " if (competencia_pagamento or "").strip() else ""
            params_tail = [str(competencia_pagamento).strip()] if comp_sql else []

            rows = cur.execute(
                f"""
                SELECT id, obrigacao_id,
                       {expr_valor_doc} AS valor_documento,
                       COALESCE(vencimento, data_evento) AS vcto
                  FROM contas_a_pagar_mov
                 WHERE (tipo_obrigacao = ? OR tipo_origem = ?)
                   AND {aberto_where}
                   AND LOWER(TRIM(credor)) = LOWER(TRIM(?))
                   {comp_sql}
                 ORDER BY DATE(vcto) ASC, id ASC
                """,
                (tipo_alvo, tipo_alvo, pagamento_destino, *params_tail),
            ).fetchall()

        elif pagamento_tipo == "Boletos":
            tipo_alvo = "BOLETO"
            rows = cur.execute(
                f"""
                SELECT id, obrigacao_id,
                       {expr_valor_doc} AS valor_documento,
                       COALESCE(vencimento, data_evento) AS vcto
                  FROM contas_a_pagar_mov
                 WHERE (tipo_obrigacao = ? OR tipo_origem = ?)
                   AND {aberto_where}
                   AND LOWER(TRIM(credor)) = LOWER(TRIM(?))
                 ORDER BY DATE(vcto) ASC, id ASC
                """,
                (tipo_alvo, tipo_alvo, pagamento_destino),
            ).fetchall()

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

            # Se a competência foi especificada, para na primeira fatura daquela competência
            if competencia_pagamento:
                break

        logger.debug(
            "Auto-baixa %s destino=%s consumido=%.2f restante=%.2f",
            tipo_alvo, pagamento_destino, float(valor_total) - restante, restante
        )
        return restante
