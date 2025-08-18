import pandas as pd
import sqlite3
import calendar
from typing import Optional, Tuple, List, Dict
from datetime import date, datetime, timedelta

from shared.db import get_conn
from shared.ids import (
    sanitize,
    uid_saida_dinheiro,
    uid_saida_bancaria,
    uid_credito_programado,
    uid_boleto_programado,
)
from repository.movimentacoes_repository import MovimentacoesRepository
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository
from repository.cartoes_repository import CartoesRepository


class LedgerService:
    """
    Servico para registrar saidas com idempotencia:
      - DINHEIRO  -> ajusta saldos_caixas e loga movimentacoes_bancarias
      - PIX/DEBITO-> ajusta saldos_bancos (coluna dinamica) e loga movimentacoes_bancarias
      - CREDITO   -> agrega LANCAMENTOS por fatura (cartao+competencia) em contas_a_pagar_mov e loga movimentacoes (programadas)
      - BOLETO    -> cria LANCAMENTOS em contas_a_pagar_mov (parcelas futuras), loga movimentacoes (programadas)
      - Classificacao de titulos em contas_a_pagar_mov com base em Pagamentos (tipo + destino)
      - Auto-baixa de FATURA_CARTAO, BOLETO e EMPRESTIMO (atualiza status)
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.mov_repo = MovimentacoesRepository(db_path)
        self.cap_repo = ContasAPagarMovRepository(db_path)
        self.cartoes_repo = CartoesRepository(db_path)

    # ================= infra =================

    def _garantir_linha_saldos_caixas(self, conn, data: str):
        cur = conn.execute("SELECT 1 FROM saldos_caixas WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("""
                INSERT INTO saldos_caixas (data, caixa, caixa_2, caixa_vendas, caixa2_dia, caixa_total, caixa2_total)
                VALUES (?, 0, 0, 0, 0, 0, 0)
            """, (data,))

    def _garantir_linha_saldos_bancos(self, conn, data: str):
        cur = conn.execute("SELECT 1 FROM saldos_bancos WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("INSERT OR IGNORE INTO saldos_bancos (data) VALUES (?)", (data,))

    def _ajustar_banco_dynamic(self, conn, banco_col: str, delta: float, data: str):
        cols = [r[1] for r in conn.execute("PRAGMA table_info(saldos_bancos)").fetchall()]
        if banco_col not in cols:
            conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_col}" REAL DEFAULT 0.0;')
        self._garantir_linha_saldos_bancos(conn, data)
        conn.execute(
            f'UPDATE saldos_bancos SET "{banco_col}" = COALESCE("{banco_col}",0) + ? WHERE data = ?',
            (float(delta), data)
        )

    @staticmethod
    def _add_months(dt: date, months: int) -> date:
        y = dt.year + (dt.month - 1 + months) // 12
        m = (dt.month - 1 + months) % 12 + 1
        d = min(dt.day, [31,
                         29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28,
                         31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
        return date(y, m, d)

    # ============== helpers CAPM / saldo / status ==============

    def _open_predicate_capm(self) -> str:
        return "COALESCE(status, 'Em aberto') = 'Em aberto'"

    def _expr_valor_documento(self, conn) -> str:
        return "COALESCE(valor_evento, 0)"

    def _expr_valor_pago(self, conn) -> str:
        return "COALESCE(valor_evento, 0)"

    def _total_pago_acumulado(self, conn, obrigacao_id: int) -> float:
        cur = conn.cursor()
        soma = cur.execute("""
            SELECT COALESCE(SUM(
                CASE
                    WHEN UPPER(COALESCE(categoria_evento,'')) LIKE 'PAGAMENTO%' THEN -valor_evento
                    ELSE 0
                END
            ), 0)
            FROM contas_a_pagar_mov
            WHERE obrigacao_id = ?
        """, (int(obrigacao_id),)).fetchone()[0]
        return float(soma or 0.0)

    # ===== NOVO: saldo e detecção de pagamento =====

    def _saldo_obrigacao(self, conn, obrigacao_id: int) -> float:
        cur = conn.cursor()
        s = cur.execute("""
            SELECT COALESCE(SUM(COALESCE(valor_evento,0)),0)
            FROM contas_a_pagar_mov
            WHERE obrigacao_id = ?
        """, (int(obrigacao_id),)).fetchone()[0]
        return float(s or 0.0)

    def _tem_pagamento(self, conn, obrigacao_id: int) -> bool:
        cur = conn.cursor()
        n = cur.execute("""
            SELECT COUNT(1)
            FROM contas_a_pagar_mov
            WHERE obrigacao_id = ?
              AND UPPER(COALESCE(categoria_evento,'')) = 'PAGAMENTO'
              AND COALESCE(valor_evento,0) <> 0
        """, (int(obrigacao_id),)).fetchone()[0]
        return int(n or 0) > 0

    def _atualizar_status_por_id(self, conn, row_id: int, obrigacao_id: int, _valor_doc_ignorado: float = 0.0) -> None:
        """
        Define status olhando o SALDO agregado da obrigação:
         - Quitado: saldo ≈ 0
         - Parcial: saldo > 0 e já houve algum pagamento
         - Em aberto: saldo > 0 e ainda não houve pagamento
        """
        eps = 0.005
        saldo = self._saldo_obrigacao(conn, int(obrigacao_id))
        if abs(saldo) <= eps:
            novo = "Quitado"
        else:
            novo = "Parcial" if self._tem_pagamento(conn, int(obrigacao_id)) else "Em aberto"
        conn.execute("UPDATE contas_a_pagar_mov SET status = ? WHERE id = ?", (novo, int(row_id)))

    def _atualizar_status_por_obrigacao(self, conn, obrigacao_id: int) -> None:
        """
        Atualiza o status de TODOS os LANCAMENTOS desta obrigação com base no saldo agregado.
        """
        eps = 0.005
        saldo = self._saldo_obrigacao(conn, int(obrigacao_id))
        if abs(saldo) <= eps:
            novo = "Quitado"
        else:
            novo = "Parcial" if self._tem_pagamento(conn, int(obrigacao_id)) else "Em aberto"

        conn.execute("""
            UPDATE contas_a_pagar_mov
               SET status = ?
             WHERE obrigacao_id = ?
               AND categoria_evento = 'LANCAMENTO'
        """, (novo, int(obrigacao_id)))

    # ============== pagamento direto por OBRIGACAO (fatura) ==============

    def _pagar_fatura_por_obrigacao(self, conn, *, obrigacao_id: int, valor: float,
                                    data_evento: str, forma_pagamento: str,
                                    origem: str, ledger_id: int, usuario: str) -> float:
        """
        Faz pagamento direto em uma fatura (obrigacao_id do LANCAMENTO de FATURA_CARTAO).
        Retorna 'sobra' (valor - pagar), se houver.
        """
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

    # ============== classificacao titulos por destino ==============

    def _classificar_conta_a_pagar_por_destino(self, conn, pagamento_tipo: Optional[str], pagamento_destino: Optional[str]) -> int:
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

    # =================== AUTO-BAIXA: Empréstimos por DESTINO ===================
    def _auto_baixar_pagamentos_emprestimo(
        self,
        conn,
        *,
        data: str,                 # 'YYYY-MM-DD'
        total_saida: float,        # valor disponível (já debitado do caixa/banco)
        forma_pagamento: str,      # 'DINHEIRO' | 'PIX' | 'DÉBITO' | 'TRANSFERÊNCIA'
        origem: str,               # 'Caixa'/'Caixa 2' ou nome do banco
        destino: str,              # rótulo do empréstimo (banco/descrição) escolhido na UI
        usuario: str,
        ledger_id: int             # id da movimentação/saída para vincular no CAP
    ) -> list[int]:
        """
        Quita parcelas de EMPRESTIMO para um 'destino' (credor) em ordem de vencimento,
        até consumir 'total_saida'. Cada parcela é um obrigacao_id em contas_a_pagar_mov.
        Retorna a lista de ids de eventos (PAGAMENTO) criados em CAP.
        """
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

    # ============== auto-baixa (fatura/boletos/emprestimo) + status ==============

    def _auto_baixar_pagamentos(self, conn, *,
                                pagamento_tipo: str,
                                pagamento_destino: str,
                                valor_total: float,
                                data_evento: str,
                                forma_pagamento: str,
                                origem: str,
                                ledger_id: int,
                                usuario: str,
                                competencia_pagamento: str | None = None) -> float:
        """
        Consome 'valor_total' quitando titulos EM ABERTO e atualiza status.
        - Fatura Cartao de Credito: casa por (cartao + [competencia opcional]) -> sem FIFO quando competencia informada
        - Boletos: casa por credor (FIFO por vencimento)
        - Empréstimos e Financiamentos: credor (FIFO por vencimento) via vw_cap_em_aberto
        Retorna o valor que SOBROU.
        """
        restante = float(valor_total)
        if restante <= 0 or not pagamento_tipo or not (pagamento_destino or "").strip():
            return restante

        tipo_norm = (pagamento_tipo or "").strip().lower()

        # ---- Caso EMPRESTIMO: usamos o helper dedicado (consome tudo FIFO) ----
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
                # como o helper usa exatamente o valor disponível, consideramos a sobra 0
                # (se quiser retornar sobra exata, teria que ler novamente os saldos pagos)
                return 0.0 if eventos else restante
            except Exception:
                return restante

        # ---- Fatura e Boletos (lógica já existente) ----
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

    # =================== DINHEIRO ===================

    def registrar_saida_dinheiro(
        self,
        *,
        data: str,
        valor: float,
        origem_dinheiro: str,
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        trans_uid: Optional[str] = None,
        vinculo_pagamento: Optional[Dict] = None,
        pagamento_tipo: Optional[str] = None,
        pagamento_destino: Optional[str] = None,
        competencia_pagamento: Optional[str] = None,
        obrigacao_id_fatura: Optional[int] = None,
    ) -> Tuple[int, int]:
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if origem_dinheiro not in ("Caixa", "Caixa 2"):
            raise ValueError("Origem do dinheiro invalida (use 'Caixa' ou 'Caixa 2').")

        categoria     = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao     = sanitize(descricao)
        usuario       = sanitize(usuario)

        trans_uid = trans_uid or uid_saida_dinheiro(
            data, valor, origem_dinheiro, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return (-1, -1)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()
            self._garantir_linha_saldos_caixas(conn, data)

            # (1) INSERT saida
            cur.execute("""
                INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                   Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                   Origem_Dinheiro, Banco_Saida)
                VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
            """, (data, categoria, sub_categoria, descricao, float(valor), usuario, origem_dinheiro))
            id_saida = int(cur.lastrowid)

            # (2) Ajusta saldos de caixa
            campo = "caixa" if origem_dinheiro == "Caixa" else "caixa_2"
            cur.execute(f"""
                UPDATE saldos_caixas SET {campo} = COALESCE({campo},0) - ?
                WHERE data = ?
            """, (float(valor), data))

            # (3) Log
            obs = f"Saida {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'saida', ?, 'saidas', ?, 'saida', ?, ?)
            """, (data, origem_dinheiro, float(valor), obs, id_saida, trans_uid))
            id_mov = int(cur.lastrowid)

            # --- PRIORITÁRIO: pagamento direto pela fatura escolhida na UI ---
            if obrigacao_id_fatura:
                sobra = self._pagar_fatura_por_obrigacao(
                    conn,
                    obrigacao_id=int(obrigacao_id_fatura),
                    valor=float(valor),
                    data_evento=data,
                    forma_pagamento="DINHEIRO",
                    origem=origem_dinheiro,
                    ledger_id=id_saida,
                    usuario=usuario
                )
                if sobra > 0:
                    cur.execute("""
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                    """, (float(sobra), id_mov))
                conn.commit()
                return (id_saida, id_mov)

            # (4) Vinculo direto com um titulo (opcional)
            if vinculo_pagamento:
                obrig_id = int(vinculo_pagamento["obrigacao_id"])
                tipo_obrig = str(vinculo_pagamento["tipo_obrigacao"])
                val = float(vinculo_pagamento.get("valor_pagar", vinculo_pagamento.get("valor_pago", valor)))

                self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id = obrig_id,
                    tipo_obrigacao = tipo_obrig,
                    valor_pago = val,
                    data_evento = data,
                    forma_pagamento = "DINHEIRO",
                    origem = origem_dinheiro,
                    ledger_id = id_saida,
                    usuario = usuario
                )
                self._atualizar_status_por_obrigacao(conn, obrig_id)

            # (5) Classificacao + Auto-baixa por destino/tipo (Fatura/Boletos/Empréstimos)
            if pagamento_tipo and pagamento_destino:
                self._classificar_conta_a_pagar_por_destino(conn, pagamento_tipo, pagamento_destino)
                restante = self._auto_baixar_pagamentos(
                    conn,
                    pagamento_tipo=pagamento_tipo,
                    pagamento_destino=pagamento_destino,
                    valor_total=float(valor),
                    data_evento=data,
                    forma_pagamento="DINHEIRO",
                    origem=origem_dinheiro,
                    ledger_id=id_saida,
                    usuario=usuario,
                    competencia_pagamento=competencia_pagamento
                )
                if restante > 0:
                    cur.execute("""
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                    """, (float(restante), id_mov))

            conn.commit()
            return (id_saida, id_mov)

    # =================== PIX / DEBITO ===================

    def registrar_saida_bancaria(
        self,
        *,
        data: str,
        valor: float,
        banco_nome: str,
        forma: str,               # "PIX" ou "DÉBITO"
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        trans_uid: Optional[str] = None,
        vinculo_pagamento: Optional[Dict] = None,
        pagamento_tipo: Optional[str] = None,
        pagamento_destino: Optional[str] = None,
        competencia_pagamento: Optional[str] = None,
        obrigacao_id_fatura: Optional[int] = None,
    ) -> Tuple[int, int]:
        forma_u = sanitize(forma).upper()
        if forma_u == "DEBITO":
            forma_u = "DÉBITO"
        if forma_u not in ("PIX", "DÉBITO"):
            raise ValueError("Forma invalida para saida bancaria.")
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")

        banco_nome    = sanitize(banco_nome)
        categoria     = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao     = sanitize(descricao)
        usuario       = sanitize(usuario)

        trans_uid = trans_uid or uid_saida_bancaria(
            data, valor, banco_nome, forma_u, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return (-1, -1)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # (1) INSERT saida
            cur.execute("""
                INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                   Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                   Origem_Dinheiro, Banco_Saida)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
            """, (data, categoria, sub_categoria, descricao, forma_u, float(valor), usuario, banco_nome))
            id_saida = int(cur.lastrowid)

            # (2) Ajusta saldos de bancos
            self._garantir_linha_saldos_bancos(conn, data)
            self._ajustar_banco_dynamic(conn, banco_nome, -float(valor), data)

            # (3) Log
            obs = f"Saida {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'saida', ?, 'saidas', ?, 'saida', ?, ?)
            """, (data, banco_nome, float(valor), obs, id_saida, trans_uid))
            id_mov = int(cur.lastrowid)

            # --- PRIORITÁRIO: pagamento direto pela fatura escolhida na UI ---
            if obrigacao_id_fatura:
                sobra = self._pagar_fatura_por_obrigacao(
                    conn,
                    obrigacao_id=int(obrigacao_id_fatura),
                    valor=float(valor),
                    data_evento=data,
                    forma_pagamento=forma_u,
                    origem=banco_nome,
                    ledger_id=id_saida,
                    usuario=usuario
                )
                if sobra > 0:
                    cur.execute("""
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                    """, (float(sobra), id_mov))
                conn.commit()
                return (id_saida, id_mov)

            # (4) Vinculo direto com um titulo (opcional)
            if vinculo_pagamento:
                obrig_id = int(vinculo_pagamento["obrigacao_id"])
                tipo_obrig = str(vinculo_pagamento["tipo_obrigacao"])
                val = float(vinculo_pagamento.get("valor_pagar", vinculo_pagamento.get("valor_pago", valor)))

                self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id = obrig_id,
                    tipo_obrigacao = tipo_obrig,
                    valor_pago = val,
                    data_evento = data,
                    forma_pagamento = forma_u,
                    origem = banco_nome,
                    ledger_id = id_saida,
                    usuario = usuario
                )
                self._atualizar_status_por_obrigacao(conn, obrig_id)

            # (5) Classificacao + Auto-baixa por destino/tipo (Fatura/Boletos/Empréstimos)
            if pagamento_tipo and pagamento_destino:
                self._classificar_conta_a_pagar_por_destino(conn, pagamento_tipo, pagamento_destino)
                restante = self._auto_baixar_pagamentos(
                    conn,
                    pagamento_tipo=pagamento_tipo,
                    pagamento_destino=pagamento_destino,
                    valor_total=float(valor),
                    data_evento=data,
                    forma_pagamento=forma_u,
                    origem=banco_nome,
                    ledger_id=id_saida,
                    usuario=usuario,
                    competencia_pagamento=competencia_pagamento
                )
                if restante > 0:
                    cur.execute("""
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                    """, (float(restante), id_mov))

            conn.commit()
            return (id_saida, id_mov)

    # =================== CREDITO (programado em fatura) ===================

    def registrar_saida_credito(
        self,
        *,
        data_compra: str,
        valor: float,
        parcelas: int,
        cartao_nome: str,
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        fechamento: int,   # ignorado (usamos do cartão no banco)
        vencimento: int,   # ignorado (usamos do cartão no banco)
        trans_uid: Optional[str] = None
    ) -> Tuple[List[int], int]:
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if int(parcelas) < 1:
            raise ValueError("Quantidade de parcelas invalida.")

        cartao_nome   = sanitize(cartao_nome)
        categoria     = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao     = sanitize(descricao)
        usuario       = sanitize(usuario)

        # usamos o mesmo UID da transação como agrupador das parcelas (purchase_uid)
        trans_uid = trans_uid or uid_credito_programado(
            data_compra, valor, parcelas, cartao_nome, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return ([], -1)

        compra = pd.to_datetime(data_compra)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # pega config real do cartao (dia de vencimento e dias de fechamento)
            vencimento_dia, dias_fechamento = self.cartoes_repo.obter_por_nome(cartao_nome)

            # competencia base da compra (fechamento = vencimento - dias_fechamento)
            comp_base_str = self._competencia_compra(
                compra_dt=pd.to_datetime(compra).to_pydatetime(),
                vencimento_dia=vencimento_dia,
                dias_fechamento=dias_fechamento
            )
            comp_base = pd.to_datetime(comp_base_str + "-01")

            # parcelas e ajuste
            valor_parc = round(float(valor) / int(parcelas), 2)
            ajuste = round(float(valor) - valor_parc * int(parcelas), 2)

            lanc_ids: List[int] = []
            total_programado = 0.0

            for p in range(1, int(parcelas) + 1):
                comp_dt = (comp_base + pd.DateOffset(months=p-1))
                y, m = comp_dt.year, comp_dt.month
                last = calendar.monthrange(y, m)[1]
                venc_d = min(int(vencimento_dia), last)
                vcto_date = datetime(y, m, venc_d).date()
                competencia = f"{y:04d}-{m:02d}"

                vparc = valor_parc + (ajuste if p == int(parcelas) else 0.0)

                # agrega esta parcela NA fatura mensal (cartao + competencia)
                lanc_id = self._add_valor_fatura(
                    conn,
                    cartao_nome=cartao_nome,
                    competencia=competencia,
                    valor_add=float(vparc),
                    data_evento=str(compra.date()),
                    vencimento=str(vcto_date),
                    usuario=usuario,
                    descricao=descricao or f"Fatura {cartao_nome} {competencia}",
                    parcela_num=p,                   # mantém como antes
                    parcelas_total=int(parcelas)     # mantém como antes
                )
                lanc_ids.append(int(lanc_id))
                total_programado += float(vparc)

                # ============================================
                # NOVO: gravar item demonstrativo da fatura
                # ============================================
                cur.execute("""
                    INSERT INTO fatura_cartao_itens
                        (purchase_uid, cartao, competencia, data_compra, descricao_compra, categoria,
                        parcela_num, parcelas, valor_parcela, usuario)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trans_uid,                          # agrupa todas as parcelas desta compra
                    cartao_nome,
                    competencia,                        # YYYY-MM desta parcela
                    str(compra.date()),                 # data original da compra
                    descricao or "",                    # descrição digitada
                    (f"{categoria or ''}" + (f" / {sub_categoria}" if sub_categoria else "")).strip(" /"),
                    int(p),                             
                    int(parcelas),
                    float(vparc),                       # valor desta parcela
                    usuario
                ))

            # log da programacao de credito (mantido)
            obs = f"Despesa CREDITO {cartao_nome} {parcelas}x - {categoria}/{sub_categoria}"
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'saida', ?, 'saidas_credito_programada', ?, 'contas_a_pagar_mov', ?, ?)
            """, (str(compra.date()), cartao_nome, float(total_programado), obs,
                lanc_ids[0] if lanc_ids else None, trans_uid))
            id_mov = int(cur.lastrowid)

            conn.commit()
            return (lanc_ids, id_mov)

    # =================== BOLETO (programado) ===================

    def registrar_saida_boleto(
        self,
        *,
        data_compra: str,
        valor: float,
        parcelas: int,
        vencimento_primeira: str,
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        fornecedor: Optional[str],
        documento: Optional[str],
        trans_uid: Optional[str] = None
    ) -> Tuple[List[int], int]:
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if int(parcelas) < 1:
            raise ValueError("Quantidade de parcelas invalida.")

        categoria     = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao     = sanitize(descricao)
        usuario       = sanitize(usuario)
        fornecedor    = sanitize(fornecedor)
        documento     = sanitize(documento)

        trans_uid = trans_uid or uid_boleto_programado(
            data_compra, valor, parcelas, vencimento_primeira, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return ([], -1)

        compra = pd.to_datetime(data_compra)
        venc1  = pd.to_datetime(vencimento_primeira)
        valor_parc = round(float(valor) / int(parcelas), 2)
        ajuste = round(float(valor) - valor_parc * int(parcelas), 2)

        ids_mov_cap: List[int] = []
        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            base_obrig_id = self.cap_repo.proximo_obrigacao_id(conn)

            for p in range(1, int(parcelas) + 1):
                vcto = (venc1 + pd.DateOffset(months=p-1)).date()
                vparc = valor_parc + (ajuste if p == int(parcelas) else 0.0)

                obrigacao_id = base_obrig_id + (p - 1)
                lanc_id = self.cap_repo.registrar_lancamento(
                    conn,
                    obrigacao_id=obrigacao_id,
                    tipo_obrigacao="BOLETO",
                    valor_total=float(vparc),
                    data_evento=str(compra.date()),
                    vencimento=str(vcto),
                    descricao=descricao or f"{fornecedor or 'Fornecedor'} {p}/{int(parcelas)} - {categoria}/{sub_categoria}",
                    credor=fornecedor,
                    competencia=str(vcto)[:7],
                    parcela_num=p,
                    parcelas_total=int(parcelas),
                    usuario=usuario
                )
                ids_mov_cap.append(int(lanc_id))

            # marca origem + status 'Em aberto'
            cur.execute("""
                UPDATE contas_a_pagar_mov
                   SET tipo_origem='BOLETO',
                       cartao_id=NULL,
                       emprestimo_id=NULL,
                       status = COALESCE(NULLIF(status,''), 'Em aberto')
                 WHERE obrigacao_id BETWEEN ? AND ?
            """, (base_obrig_id, base_obrig_id + int(parcelas) - 1))

            obs = f"Boleto {parcelas}x - {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, 'Boleto', 'saida', ?, 'saidas_boleto_programada', ?, 'contas_a_pagar_mov', ?, ?)
            """, (str(compra.date()), float(valor), obs, ids_mov_cap[0] if ids_mov_cap else None, trans_uid))
            id_mov = int(cur.lastrowid)

            conn.commit()
            return (ids_mov_cap, id_mov)

    # =================== fatura mensal (helper interno) ===================

    def _add_valor_fatura(self, conn, *, cartao_nome: str, competencia: str,
                          valor_add: float, data_evento: str, vencimento: str,
                          usuario: str, descricao: str | None,
                          parcela_num: int | None = None,
                          parcelas_total: int | None = None) -> int:
        """
        Garante uma fatura mensal (um LANCAMENTO) por cartao+competencia:
        - Se já existir, soma valor_add no mesmo LANCAMENTO (não mexe parcela_num/parcelas_total).
        - Se não existir, cria o LANCAMENTO com valor_add e define parcela_num/parcelas_total.
        Retorna o id do LANCAMENTO.
        """
        cur = conn.cursor()

        row = cur.execute("""
            SELECT id, obrigacao_id, COALESCE(valor_evento,0.0) AS valor_atual
              FROM contas_a_pagar_mov
             WHERE tipo_obrigacao='FATURA_CARTAO'
               AND categoria_evento='LANCAMENTO'
               AND LOWER(TRIM(credor)) = LOWER(TRIM(?))
               AND competencia = ?
             LIMIT 1
        """, (cartao_nome, competencia)).fetchone()

        if row:
            lanc_id = int(row[0])
            obrigacao_id = int(row[1])
            cur.execute("""
                UPDATE contas_a_pagar_mov
                   SET valor_evento = COALESCE(valor_evento,0) + ?,
                       descricao = COALESCE(descricao, ?)
                 WHERE id = ?
            """, (float(valor_add), descricao, lanc_id))
        else:
            obrigacao_id = self.cap_repo.proximo_obrigacao_id(conn)
            lanc_id = self.cap_repo.registrar_lancamento(
                conn,
                obrigacao_id=obrigacao_id,
                tipo_obrigacao="FATURA_CARTAO",
                valor_total=float(valor_add),
                data_evento=data_evento,
                vencimento=vencimento,
                descricao=descricao or f"Fatura {cartao_nome} {competencia}",
                credor=cartao_nome,
                competencia=competencia,
                parcela_num=int(parcela_num) if parcela_num is not None else 1,
                parcelas_total=int(parcelas_total) if parcelas_total is not None else 1,
                usuario=usuario
            )
            cur.execute("""
                UPDATE contas_a_pagar_mov
                   SET tipo_origem='FATURA_CARTAO',
                       cartao_id = (SELECT id FROM cartoes_credito WHERE LOWER(TRIM(nome)) = LOWER(TRIM(?)) LIMIT 1),
                       status = COALESCE(NULLIF(status,''), 'Em aberto')
                 WHERE id = ?
            """, (cartao_nome, lanc_id))

        row2 = cur.execute("SELECT id, obrigacao_id, COALESCE(valor_evento,0) FROM contas_a_pagar_mov WHERE id=?",
                           (lanc_id,)).fetchone()
        valor_doc = float(row2[2])
        self._atualizar_status_por_id(conn, lanc_id, obrigacao_id, valor_doc)

        return lanc_id

    # =================== regra de competência do cartão ===================

    def _competencia_compra(self, compra_dt: datetime, vencimento_dia: int, dias_fechamento: int) -> str:
        """
        Regra: fechamento = data(vencimento) - dias_fechamento.
        - Compra NO dia de fechamento fica no MÊS ATUAL.
        - Compra DEPOIS do fechamento vai para o PRÓXIMO mês.
        Retorna 'YYYY-MM'.
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

    # =================== BOLETO (pagamento de PARCELA) ===================
    def pagar_parcela_boleto(
        self,
        *,
        data: str,                       # 'YYYY-MM-DD'
        valor: float,                    # valor que o usuário pretende pagar (pode ser > saldo; será limitado)
        forma_pagamento: str,            # 'DINHEIRO', 'PIX', 'DÉBITO', 'TRANSFERÊNCIA'
        origem: str,                     # 'Caixa' / 'Caixa 2' (dinheiro) ou nome da coluna do banco (bancário)
        obrigacao_id: int,               # obrigação (parcela) selecionada na UI
        usuario: str,
        categoria: Optional[str] = "Boletos",
        sub_categoria: Optional[str] = None,
        descricao: Optional[str] = None,
        trans_uid: Optional[str] = None,
        descricao_extra_cap: Optional[str] = None,   # ignorado (não criaremos mais linhas extras)
        multa: float = 0.0,
        juros: float = 0.0,
        desconto: float = 0.0,
    ) -> tuple[int, int, int]:
        """
        Fluxo (definitivo, sem linhas extras):
        1) Calcula total_saida = valor_a_pagar + juros + multa - desconto
        2) Debita do Caixa/Banco e loga movimentações com total_saida
        3) Registra um único PAGAMENTO em contas_a_pagar_mov com valor_evento = -total_saida
        4) Aplica na própria PARCELA: acumula valor_pago_acumulado/juros/multa/desconto e define status
        * Não cria eventos MULTA/JUROS/DESCONTO; tudo vai para as colunas acumuladoras.
        * Não recalcula status pelo saldo agregado (evita voltar para 'Parcial').
        """
        v_pg = max(0.0, float(valor))
        v_multa = max(0.0, float(multa or 0.0))
        v_juros = max(0.0, float(juros or 0.0))
        v_desc  = max(0.0, float(desconto or 0.0))

        cat  = sanitize(categoria)
        sub  = sanitize(sub_categoria)
        desc = sanitize(descricao)
        usu  = sanitize(usuario)
        org  = sanitize(origem)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # ====== 1) Limitar pagamento ao saldo do título ======
            saldo_atual = self.cap_repo.obter_saldo_obrigacao(conn, int(obrigacao_id))
            eps = 0.005
            valor_a_pagar = min(v_pg, max(saldo_atual, 0.0))

            # total efetivamente desembolsado (principal pago + encargos - descontos)
            total_saida = max(valor_a_pagar + v_multa + v_juros - v_desc, 0.0)

            resumo_aj = []
            if v_multa > 0: resumo_aj.append(f"multa R$ {v_multa:.2f}")
            if v_juros > 0: resumo_aj.append(f"juros R$ {v_juros:.2f}")
            if v_desc  > 0: resumo_aj.append(f"desconto R$ {v_desc:.2f}")
            obs_extra = (" | " + ", ".join(resumo_aj)) if resumo_aj else ""

            # ====== 2) Debitar do Caixa/Banco + 'saida' + log ======
            if forma_pagamento == "DINHEIRO":
                self._garantir_linha_saldos_caixas(conn, data)
                cur.execute("""
                    INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                    Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                    Origem_Dinheiro, Banco_Saida)
                    VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
                """, (data, cat, sub, desc, total_saida, usu, org))
                id_saida = int(cur.lastrowid)

                campo = "caixa" if org == "Caixa" else "caixa_2"
                cur.execute(f"""
                    UPDATE saldos_caixas SET {campo} = COALESCE({campo},0) - ?
                    WHERE data = ?
                """, (total_saida, data))

                obs = (f"Pagamento Boleto {cat}/{sub or ''}".strip()
                    + (f" - {desc}" if desc else "")
                    + obs_extra)
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                    VALUES (?, ?, 'saida', ?, 'saidas_boleto_pagamento', ?, 'saida', ?, ?)
                """, (data, org, total_saida, obs, id_saida, trans_uid))
                id_mov = int(cur.lastrowid)
            else:
                self._ajustar_banco_dynamic(conn, banco_col=org, delta=-total_saida, data=data)
                cur.execute("""
                    INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                    Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                    Origem_Dinheiro, Banco_Saida)
                    VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
                """, (data, cat, sub, desc, forma_pagamento, total_saida, usu, org))
                id_saida = int(cur.lastrowid)

                obs = (f"Pagamento Boleto {cat}/{sub or ''}".strip()
                    + (f" - {desc}" if desc else "")
                    + obs_extra)
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                    VALUES (?, ?, 'saida', ?, 'saidas_boleto_pagamento', ?, 'saida', ?, ?)
                """, (data, org, total_saida, obs, id_saida, trans_uid))
                id_mov = int(cur.lastrowid)

            # ====== 3) Registrar um único PAGAMENTO (negativo) no total desembolsado ======
            evento_id = 0
            if total_saida > eps:
                evento_id = self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id=int(obrigacao_id),
                    tipo_obrigacao="BOLETO",
                    valor_pago=float(total_saida),   # <- total (principal + encargos - desconto)
                    data_evento=data,
                    forma_pagamento=forma_pagamento,
                    origem=org,
                    ledger_id=id_saida,
                    usuario=usu,
                )

            # ====== 4) Aplicar na própria PARCELA (acumuladores + status) ======
            row = conn.execute("""
                SELECT id, COALESCE(valor_evento,0) AS valor_parcela
                FROM contas_a_pagar_mov
                WHERE obrigacao_id = ?
                AND categoria_evento = 'LANCAMENTO'
                LIMIT 1
            """, (int(obrigacao_id),)).fetchone()
            if not row:
                raise ValueError(f"Parcela (obrigacao_id={obrigacao_id}) não encontrada.")

            parcela_id = int(row[0])
            valor_parcela = float(row[1])

            # Atualiza acumuladores e status com base no total desembolsado
            self.cap_repo.aplicar_pagamento_parcela(
                conn,
                parcela_id=parcela_id,
                valor_parcela=valor_parcela,
                valor_pago_total=float(total_saida),
                juros=float(v_juros),
                multa=float(v_multa),
                desconto=float(v_desc),
            )

            # Importante: NÃO chamar _atualizar_status_por_obrigacao aqui.

            conn.commit()
            return (id_saida, id_mov, int(evento_id))

    # =================== FATURA CARTÃO (pagamento com ajustes) ===================
    def pagar_fatura_cartao(
        self,
        *,
        data: str,                       # 'YYYY-MM-DD'
        valor: float,                    # valor que o usuário pretende pagar (pode ser > saldo; será limitado)
        forma_pagamento: str,            # 'DINHEIRO', 'PIX', 'DÉBITO', 'TRANSFERÊNCIA'
        origem: str,                     # 'Caixa' / 'Caixa 2' (dinheiro) ou nome da coluna do banco (bancário)
        obrigacao_id: int,               # obrigação (fatura) selecionada na UI
        usuario: str,
        categoria: Optional[str] = "Fatura Cartão de Crédito",
        sub_categoria: Optional[str] = None,
        descricao: Optional[str] = None,
        trans_uid: Optional[str] = None,
        multa: float = 0.0,
        juros: float = 0.0,
        desconto: float = 0.0,
    ) -> tuple[int, int, int]:
        """
        Fluxo (mesma filosofia do boleto, sem linhas extras):
        1) Calcula total_saida = valor_a_pagar + juros + multa - desconto
        2) Debita do Caixa/Banco e loga movimentações com total_saida
        3) Registra um único PAGAMENTO em contas_a_pagar_mov com valor_evento = -total_saida
        4) Aplica na PRÓPRIA FATURA (linha LANCAMENTO): acumula valor_pago_acumulado/juros/multa/desconto e define status
        * Não cria eventos MULTA/JUROS/DESCONTO; tudo vai para as colunas acumuladoras.
        * Não recalcula status pelo saldo agregado (evita voltar para 'Parcial').
        """
        v_pg = max(0.0, float(valor))
        v_multa = max(0.0, float(multa or 0.0))
        v_juros = max(0.0, float(juros or 0.0))
        v_desc  = max(0.0, float(desconto or 0.0))

        cat  = sanitize(categoria)
        sub  = sanitize(sub_categoria)
        desc = sanitize(descricao)
        usu  = sanitize(usuario)
        org  = sanitize(origem)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # Localiza a linha LANCAMENTO da fatura
            row = cur.execute("""
                SELECT id, COALESCE(valor_evento,0.0) AS valor_parcela
                  FROM contas_a_pagar_mov
                 WHERE obrigacao_id = ?
                   AND categoria_evento = 'LANCAMENTO'
                   AND (tipo_obrigacao='FATURA_CARTAO' OR tipo_origem='FATURA_CARTAO')
                 LIMIT 1
            """, (int(obrigacao_id),)).fetchone()
            if not row:
                raise ValueError(f"Fatura (obrigacao_id={obrigacao_id}) não encontrada.")
            parcela_id = int(row[0])
            valor_parcela = float(row[1])

            # ====== 1) Limitar pagamento ao saldo da fatura (principal) ======
            saldo_atual = self.cap_repo.obter_saldo_obrigacao(conn, int(obrigacao_id))
            eps = 0.005
            valor_a_pagar = min(v_pg, max(saldo_atual, 0.0))

            # total efetivamente desembolsado (principal pago + encargos - descontos)
            total_saida = max(valor_a_pagar + v_multa + v_juros - v_desc, 0.0)

            resumo_aj = []
            if v_multa > 0: resumo_aj.append(f"multa R$ {v_multa:.2f}")
            if v_juros > 0: resumo_aj.append(f"juros R$ {v_juros:.2f}")
            if v_desc  > 0: resumo_aj.append(f"desconto R$ {v_desc:.2f}")
            obs_extra = (" | " + ", ".join(resumo_aj)) if resumo_aj else ""

            # ====== 2) Debitar do Caixa/Banco + 'saida' + log ======
            if forma_pagamento == "DINHEIRO":
                self._garantir_linha_saldos_caixas(conn, data)
                cur.execute("""
                    INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                       Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                       Origem_Dinheiro, Banco_Saida)
                    VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
                """, (data, cat, sub, desc, float(total_saida), usu, org))
                id_saida = int(cur.lastrowid)

                campo = "caixa" if org == "Caixa" else "caixa_2"
                cur.execute(f"""
                    UPDATE saldos_caixas SET {campo} = COALESCE({campo},0) - ?
                    WHERE data = ?
                """, (float(total_saida), data))

                obs = (f"Pagamento Fatura • {cat}/{sub or ''}".strip()
                       + (f" - {desc}" if desc else "")
                       + obs_extra)
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                    VALUES (?, ?, 'saida', ?, 'saidas_fatura_pagamento', ?, 'saida', ?, ?)
                """, (data, org, float(total_saida), obs, id_saida, trans_uid))
                id_mov = int(cur.lastrowid)
            else:
                self._ajustar_banco_dynamic(conn, banco_col=org, delta=-float(total_saida), data=data)
                cur.execute("""
                    INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                       Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                       Origem_Dinheiro, Banco_Saida)
                    VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
                """, (data, cat, sub, desc, forma_pagamento, float(total_saida), usu, org))
                id_saida = int(cur.lastrowid)

                obs = (f"Pagamento Fatura • {cat}/{sub or ''}".strip()
                       + (f" - {desc}" if desc else "")
                       + obs_extra)
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                    VALUES (?, ?, 'saida', ?, 'saidas_fatura_pagamento', ?, 'saida', ?, ?)
                """, (data, org, float(total_saida), obs, id_saida, trans_uid))
                id_mov = int(cur.lastrowid)

            # Se o usuário digitou valor acima do saldo, anota observação
            if v_pg > saldo_atual + eps:
                cur.execute("""
                    UPDATE movimentacoes_bancarias
                       SET observacao = COALESCE(observacao,'') || ' [valor ajustado ao saldo: R$ ' || printf('%.2f', ?) || ']'
                     WHERE id = ?
                """, (float(total_saida), id_mov))

            # ====== 3) Registrar um único PAGAMENTO (negativo) no total desembolsado ======
            evento_id = 0
            if total_saida > eps:
                evento_id = self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id=int(obrigacao_id),
                    tipo_obrigacao="FATURA_CARTAO",
                    valor_pago=float(total_saida),   # <- total (principal + encargos - desconto)
                    data_evento=data,
                    forma_pagamento=forma_pagamento,
                    origem=org,
                    ledger_id=id_saida,
                    usuario=usu,
                )

            # ====== 4) Aplicar na PRÓPRIA FATURA (acumuladores + status) ======
            self.cap_repo.aplicar_pagamento_parcela(
                conn,
                parcela_id=parcela_id,
                valor_parcela=float(valor_parcela),
                valor_pago_total=float(total_saida),
                juros=float(v_juros),
                multa=float(v_multa),
                desconto=float(v_desc),
            )

            conn.commit()
            return (id_saida, id_mov, int(evento_id))
        

    # =================== EMPRESTIMO (pagamento de PARCELA) ===================
    def pagar_parcela_emprestimo(
        self,
        *,
        data: str,                       # 'YYYY-MM-DD'
        valor: float,                    # valor que o usuário pretende pagar (pode ser > saldo; será limitado)
        forma_pagamento: str,            # 'DINHEIRO', 'PIX', 'DÉBITO', 'TRANSFERÊNCIA'
        origem: str,                     # 'Caixa' / 'Caixa 2' (dinheiro) ou nome da coluna do banco (bancário)
        obrigacao_id: int,               # obrigação (parcela) selecionada na UI
        usuario: str,
        categoria: Optional[str] = "Empréstimos e Financiamentos",
        sub_categoria: Optional[str] = None,
        descricao: Optional[str] = None,
        trans_uid: Optional[str] = None,
        multa: float = 0.0,
        juros: float = 0.0,
        desconto: float = 0.0,
    ) -> tuple[int, int, int]:
        """
        Fluxo idêntico ao boleto:
        1) total_saida = principal (limitado ao saldo) + juros + multa - desconto
        2) Debita do Caixa/Banco e loga movimentações com total_saida
        3) Registra PAGAMENTO em contas_a_pagar_mov (valor_evento = -total_saida)
        4) Aplica na própria PARCELA (acumuladores + status) se você estiver usando essas colunas extras.
        """
        v_pg = max(0.0, float(valor))
        v_multa = max(0.0, float(multa or 0.0))
        v_juros = max(0.0, float(juros or 0.0))
        v_desc  = max(0.0, float(desconto or 0.0))

        cat  = sanitize(categoria)
        sub  = sanitize(sub_categoria)
        desc = sanitize(descricao)
        usu  = sanitize(usuario)
        org  = sanitize(origem)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # 1) limitar ao saldo
            saldo_atual = self.cap_repo.obter_saldo_obrigacao(conn, int(obrigacao_id))
            eps = 0.005
            valor_a_pagar = min(v_pg, max(saldo_atual, 0.0))
            total_saida = max(valor_a_pagar + v_multa + v_juros - v_desc, 0.0)

            # 2) Debitar do Caixa/Banco + log + INSERT em 'saida'
            if forma_pagamento == "DINHEIRO":
                self._garantir_linha_saldos_caixas(conn, data)
                # cria a linha em 'saida'
                cur.execute("""
                    INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                       Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                       Origem_Dinheiro, Banco_Saida)
                    VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
                """, (data, cat, sub, desc, float(total_saida), usu, org))
                id_saida = int(cur.lastrowid)

                # ajusta saldo do caixa
                campo = "caixa" if org == "Caixa" else "caixa_2"
                cur.execute(f"""
                    UPDATE saldos_caixas SET {campo} = COALESCE({campo},0) - ?
                    WHERE data = ?
                """, (float(total_saida), data))

                # log
                obs = (f"Pagamento Empréstimo {cat}/{sub or ''}".strip()
                    + (f" - {desc}" if desc else "")
                    + (f" | multa R$ {v_multa:.2f}" if v_multa>0 else "")
                    + (f", juros R$ {v_juros:.2f}" if v_juros>0 else "")
                    + (f", desconto R$ {v_desc:.2f}" if v_desc>0 else ""))
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                    VALUES (?, ?, 'saida', ?, 'saidas_emprestimo_pagamento', ?, 'saida', ?, ?)
                """, (data, org, float(total_saida), obs, id_saida, trans_uid))
                id_mov = int(cur.lastrowid)

            else:
                # bancário (PIX/DÉBITO/TRANSFERÊNCIA): ajustar coluna dinâmica e inserir em 'saida'
                self._garantir_linha_saldos_bancos(conn, data)
                self._ajustar_banco_dynamic(conn, banco_col=org, delta=-float(total_saida), data=data)

                cur.execute("""
                    INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                       Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                       Origem_Dinheiro, Banco_Saida)
                    VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
                """, (data, cat, sub, desc, forma_pagamento, float(total_saida), usu, org))
                id_saida = int(cur.lastrowid)

                obs = (f"Pagamento Empréstimo {cat}/{sub or ''}".strip()
                    + (f" - {desc}" if desc else "")
                    + (f" | multa R$ {v_multa:.2f}" if v_multa>0 else "")
                    + (f", juros R$ {v_juros:.2f}" if v_juros>0 else "")
                    + (f", desconto R$ {v_desc:.2f}" if v_desc>0 else ""))
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                    VALUES (?, ?, 'saida', ?, 'saidas_emprestimo_pagamento', ?, 'saida', ?, ?)
                """, (data, org, float(total_saida), obs, id_saida, trans_uid))
                id_mov = int(cur.lastrowid)

            # 3) Evento PAGAMENTO em contas_a_pagar_mov (negativo)
            evento_id = self.cap_repo.registrar_pagamento(
                conn,
                obrigacao_id=int(obrigacao_id),
                tipo_obrigacao="EMPRESTIMO",
                valor_pago=float(total_saida),
                data_evento=data,
                forma_pagamento=forma_pagamento,
                origem=org,
                ledger_id=int(id_saida),
                usuario=usu
            )

            # 4) Atualização fina na própria parcela (se existir suporte às colunas acumuladoras)
            try:
                row = conn.execute("""
                    SELECT id, COALESCE(valor_evento,0) AS valor_parcela
                    FROM contas_a_pagar_mov
                    WHERE obrigacao_id = ?
                      AND categoria_evento = 'LANCAMENTO'
                    LIMIT 1
                """, (int(obrigacao_id),)).fetchone()
                if row:
                    parcela_id = int(row[0])
                    valor_parcela = float(row[1])
                    self.cap_repo.aplicar_pagamento_parcela(
                        conn,
                        parcela_id=int(parcela_id),
                        valor_parcela=float(valor_parcela),
                        valor_pago_total=float(total_saida),
                        juros=float(v_juros),
                        multa=float(v_multa),
                        desconto=float(v_desc),
                    )
            except Exception:
                pass

            conn.commit()
            return (id_saida, id_mov, int(evento_id))
        
    # =================== EMPRESTIMO (programar parcelas após cadastro) ===================
    def programar_emprestimo(
        self,
        *,
        credor: str,                     # rótulo do empréstimo (banco/descrição) que aparecerá em CAP
        data_primeira_parcela: str,      # 'YYYY-MM-DD'
        parcelas_total: int,             # quantidade total
        valor_parcela: float,            # valor de cada parcela (fixa)
        usuario: str,
        descricao: str | None = None,    # ex.: "Contrato 123 / CDC"
        emprestimo_id: int | None = None,# se tiver o id do cadastro (FK opcional)
        parcelas_ja_pagas: int = 0       # quantas parcelas considerar como quitadas de imediato
    ) -> tuple[list[int], list[int]]:
        """
        Gera LANCAMENTOS de EMPRESTIMO em contas_a_pagar_mov (um por parcela),
        a partir de data_primeira_parcela, mês a mês, com numeração 1..N.
        Se 'parcelas_ja_pagas' > 0, registra eventos de PAGAMENTO nas primeiras K parcelas.
        """

        # --------- Coerções/validações defensivas ---------
        credor = sanitize(credor or "").strip() or "Empréstimo"
        usuario = sanitize(usuario or "Sistema")
        desc   = sanitize(descricao)

        # normaliza data
        data_primeira_parcela = str(data_primeira_parcela or "").strip()
        if not data_primeira_parcela:
            raise ValueError("data_primeira_parcela não informada.")

        try:
            base_dt = pd.to_datetime(data_primeira_parcela)
        except Exception:
            raise ValueError(f"data_primeira_parcela inválida: {data_primeira_parcela!r}")

        try:
            parcelas_total = int(parcelas_total or 0)
        except Exception:
            parcelas_total = 0

        try:
            valor_parcela = float(valor_parcela or 0.0)
        except Exception:
            valor_parcela = 0.0

        try:
            parcelas_ja_pagas = int(parcelas_ja_pagas or 0)
        except Exception:
            parcelas_ja_pagas = 0

        if parcelas_total < 1:
            raise ValueError("parcelas_total deve ser >= 1")
        if valor_parcela <= 0:
            raise ValueError("valor_parcela deve ser > 0")

        # ---------------------------------------------------
        ids_lanc: list[int] = []
        ids_pay:  list[int] = []

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            base_obrig_id = self.cap_repo.proximo_obrigacao_id(conn)

            for i in range(1, int(parcelas_total) + 1):
                vcto = (base_dt + pd.DateOffset(months=i-1)).date()
                competencia = f"{vcto.year:04d}-{vcto.month:02d}"
                obrigacao_id = base_obrig_id + (i - 1)

                lanc_id = self.cap_repo.registrar_lancamento(
                    conn,
                    obrigacao_id=obrigacao_id,
                    tipo_obrigacao="EMPRESTIMO",
                    valor_total=float(valor_parcela),
                    data_evento=str(base_dt.date()),          # data de criação técnica do título
                    vencimento=str(vcto),                     # vencimento desta parcela
                    descricao=desc or f"{credor} {i}/{parcelas_total}",
                    credor=credor,
                    competencia=competencia,
                    parcela_num=i,
                    parcelas_total=int(parcelas_total),
                    usuario=usuario
                )
                ids_lanc.append(int(lanc_id))

            # marca origem e status 'Em aberto'; vincula emprestimo_id se fornecido
            if emprestimo_id is not None:
                try:
                    emprestimo_id = int(emprestimo_id)
                except Exception:
                    emprestimo_id = None

            if emprestimo_id is not None:
                cur.execute("""
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='EMPRESTIMO',
                           emprestimo_id=?,
                           cartao_id=NULL,
                           status = COALESCE(NULLIF(status,''), 'Em aberto')
                     WHERE obrigacao_id BETWEEN ? AND ?
                """, (int(emprestimo_id), base_obrig_id, base_obrig_id + int(parcelas_total) - 1))
            else:
                cur.execute("""
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='EMPRESTIMO',
                           cartao_id=NULL,
                           status = COALESCE(NULLIF(status,''), 'Em aberto')
                     WHERE obrigacao_id BETWEEN ? AND ?
                """, (base_obrig_id, base_obrig_id + int(parcelas_total) - 1))

            # se houver parcelas já pagas, registramos pagamentos “técnicos” (não mexe caixa/banco)
            k = max(0, min(int(parcelas_ja_pagas or 0), int(parcelas_total)))
            if k > 0:
                for i in range(0, k):
                    obrig_id = base_obrig_id + i
                    row = cur.execute("""
                        SELECT COALESCE(vencimento, data_evento) AS vcto
                          FROM contas_a_pagar_mov
                         WHERE obrigacao_id = ?
                           AND categoria_evento='LANCAMENTO'
                         LIMIT 1
                    """, (obrig_id,)).fetchone()
                    vcto = (row[0] if row and row[0] else str((base_dt + pd.DateOffset(months=i)).date()))

                    ev_id = self.cap_repo.registrar_pagamento(
                        conn,
                        obrigacao_id=int(obrig_id),
                        tipo_obrigacao="EMPRESTIMO",
                        valor_pago=float(valor_parcela),
                        data_evento=str(vcto),
                        forma_pagamento="AJUSTE",
                        origem="programacao",
                        ledger_id=None,
                        usuario=usuario
                    )
                    ids_pay.append(int(ev_id))

                # atualiza status das primeiras k para Quitado
                cur.execute("""
                    UPDATE contas_a_pagar_mov
                       SET status='Quitado'
                     WHERE obrigacao_id BETWEEN ? AND ?
                       AND categoria_evento='LANCAMENTO'
                """, (base_obrig_id, base_obrig_id + k - 1))

            # log opcional da programação (não afeta saldos)
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'info', 0, 'emprestimo_programado',
                        ?, 'contas_a_pagar_mov', ?, NULL)
            """, (
                str(base_dt.date()),
                credor,
                f"Empréstimo programado {parcelas_total}x de R$ {valor_parcela:.2f} - "
                f"{credor} (pagas na origem: {k})",
                ids_lanc[0] if ids_lanc else None
            ))

            conn.commit()

        return (ids_lanc, ids_pay)

    def programar_emprestimo_por_cadastro(
        self,
        *,
        cadastro: dict,
        usuario: str
    ) -> tuple[list[int], list[int]]:
        """
        Helper para ser chamado logo após inserir o registro em 'emprestimos_financiamentos'.
        Espera um dicionário 'cadastro' com chaves:
          - 'id' (opcional), 'banco' ou 'descricao' (usamos como credor),
          - 'data_primeira_parcela' (YYYY-MM-DD),
          - 'parcelas_total' (int),
          - 'valor_parcela' (float),
          - 'parcelas_ja_pagas' (int, opcional),
          - 'observacao' (opcional).
        """

        # Extrai e NORMALIZA com segurança
        credor = sanitize(
            cadastro.get("banco")
            or cadastro.get("descricao")
            or cadastro.get("tipo")
            or "Empréstimo"
        )

        data_primeira = str(cadastro.get("data_primeira_parcela") or "").strip()
        if not data_primeira:
            raise ValueError("data_primeira_parcela ausente no cadastro.")

        try:
            # valida a data cedo para erro mais claro
            pd.to_datetime(data_primeira)
        except Exception:
            raise ValueError(f"data_primeira_parcela inválida: {data_primeira!r}")

        try:
            pt = int(cadastro.get("parcelas_total") or 0)
        except Exception:
            pt = 0

        try:
            vp = float(cadastro.get("valor_parcela") or 0.0)
        except Exception:
            vp = 0.0

        try:
            pp = int(cadastro.get("parcelas_ja_pagas") or 0)
        except Exception:
            pp = 0

        emp_id = cadastro.get("id")
        try:
            emp_id = int(emp_id) if emp_id is not None else None
        except Exception:
            emp_id = None

        if pt < 1:
            raise ValueError(f"parcelas_total inválido: {cadastro.get('parcelas_total')!r}")
        if vp <= 0:
            raise ValueError(f"valor_parcela inválido: {cadastro.get('valor_parcela')!r}")

        # chama o gerador com valores já coeridos
        return self.programar_emprestimo(
            credor=credor,
            data_primeira_parcela=data_primeira,
            parcelas_total=pt,
            valor_parcela=vp,
            usuario=usuario,
            descricao=cadastro.get("observacao"),
            emprestimo_id=emp_id,
            parcelas_ja_pagas=pp
        )