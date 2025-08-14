import pandas as pd
import sqlite3
from typing import Optional, Tuple, List, Dict
from datetime import date

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


class LedgerService:
    """
    Serviço para registrar saídas com idempotência:
      - DINHEIRO  -> ajusta saldos_caixas e loga movimentacoes_bancarias
      - PIX/DÉBITO-> ajusta saldos_bancos (coluna dinâmica) e loga movimentacoes_bancarias
      - CRÉDITO   -> cria LANCAMENTOS em contas_a_pagar_mov (faturas futuras), loga movimentações (programadas)
      - BOLETO    -> cria LANCAMENTOS em contas_a_pagar_mov (parcelas futuras), loga movimentações (programadas)
      - Classificação opcional de títulos em contas_a_pagar_mov com base em Pagamentos (tipo + destino)
      - Auto-baixa de FATURA_CARTAO e BOLETO quando Categoria=Pagamentos (atualiza status)
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.mov_repo = MovimentacoesRepository(db_path)
        self.cap_repo = ContasAPagarMovRepository(db_path)

    # ---------- infra ----------
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

    # ---------- classificar títulos por destino ----------
    def _classificar_conta_a_pagar_por_destino(self, conn, pagamento_tipo: Optional[str], pagamento_destino: Optional[str]) -> int:
        if not pagamento_tipo or not pagamento_destino or not str(pagamento_destino).strip():
            return 0

        destino = str(pagamento_destino).strip()
        cur = conn.cursor()

        if pagamento_tipo == "Fatura Cartão de Crédito":
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

        elif pagamento_tipo == "Empréstimos e Financiamentos":
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

    # ---------- helper "em aberto" ----------
    def _open_predicate_capm(self) -> str:
        # Trata NULL como "Em aberto" para compatibilidade retroativa
        return "COALESCE(status, 'Em aberto') = 'Em aberto'"

    # ---------- auto-baixa de títulos (fatura/boletos) + status ----------
    def _auto_baixar_pagamentos(self, conn, *,
                                pagamento_tipo: str,
                                pagamento_destino: str,
                                valor_total: float,
                                data_evento: str,
                                forma_pagamento: str,
                                origem: str,
                                ledger_id: int,
                                usuario: str) -> float:
        """
        Consome 'valor_total' quitando títulos EM ABERTO (FIFO por vencimento) e atualiza status.
        - Fatura Cartão de Crédito: tenta casar por cartao_id (se houver), senão por credor.
        - Boletos: casa por credor.
        Retorna o valor que SOBROU.
        """
        restante = float(valor_total)
        if restante <= 0 or not pagamento_tipo or not (pagamento_destino or "").strip():
            return restante

        aberto_where = self._open_predicate_capm()
        cur = conn.cursor()

        if pagamento_tipo == "Fatura Cartão de Crédito":
            tipo_alvo = "FATURA_CARTAO"
            row = cur.execute("""
                SELECT id FROM cartoes_credito
                WHERE LOWER(TRIM(nome)) = LOWER(TRIM(?))
                LIMIT 1
            """, (pagamento_destino,)).fetchone()
            cartao_id = int(row[0]) if row else None

            if cartao_id is not None:
                rows = cur.execute(f"""
                    SELECT id, obrigacao_id,
                           COALESCE(valor_total, valor, valor_evento, 0) AS valor_documento,
                           COALESCE(vencimento, data_evento) AS vcto
                      FROM contas_a_pagar_mov
                     WHERE (tipo_obrigacao = ? OR tipo_origem = ?)
                       AND {aberto_where}
                       AND (
                            cartao_id = ?
                            OR (cartao_id IS NULL AND LOWER(TRIM(credor)) = LOWER(TRIM(?)))
                       )
                     ORDER BY DATE(vcto) ASC, id ASC
                """, (tipo_alvo, tipo_alvo, cartao_id, pagamento_destino)).fetchall()
            else:
                rows = cur.execute(f"""
                    SELECT id, obrigacao_id,
                           COALESCE(valor_total, valor, valor_evento, 0) AS valor_documento,
                           COALESCE(vencimento, data_evento) AS vcto
                      FROM contas_a_pagar_mov
                     WHERE (tipo_obrigacao = ? OR tipo_origem = ?)
                       AND {aberto_where}
                       AND LOWER(TRIM(credor)) = LOWER(TRIM(?))
                     ORDER BY DATE(vcto) ASC, id ASC
                """, (tipo_alvo, tipo_alvo, pagamento_destino)).fetchall()

        elif pagamento_tipo == "Boletos":
            tipo_alvo = "BOLETO"
            rows = cur.execute(f"""
                SELECT id, obrigacao_id,
                       COALESCE(valor_total, valor, valor_evento, 0) AS valor_documento,
                       COALESCE(vencimento, data_evento) AS vcto
                  FROM contas_a_pagar_mov
                 WHERE (tipo_obrigacao = ? OR tipo_origem = ?)
                   AND {aberto_where}
                   AND LOWER(TRIM(credor)) = LOWER(TRIM(?))
                 ORDER BY DATE(vcto) ASC, id ASC
            """, (tipo_alvo, tipo_alvo, pagamento_destino)).fetchall()

        else:
            # extensão para empréstimos pode ser feita depois
            return restante

        if not rows:
            raise ValueError(
                f"Nenhum título EM ABERTO encontrado para tipo='{pagamento_tipo}', destino='{pagamento_destino}'. "
                f"Verifique se há faturas/boletos em aberto e se o destino coincide."
            )

        for (row_id, obrigacao_id, valor_doc, _vcto) in rows:
            if restante <= 0:
                break

            valor_doc = float(valor_doc or 0.0)
            if valor_doc <= 0:
                continue

            pagar = min(restante, valor_doc)

            # 1) registra pagamento (usa regra de negócio do seu repository)
            self.cap_repo.registrar_pagamento(
                conn,
                obrigacao_id=int(obrigacao_id),
                tipo_obrigacao=tipo_alvo,
                valor_pago=float(pagar),
                data_evento=data_evento,
                forma_pagamento=forma_pagamento,
                origem=origem,
                ledger_id=int(ledger_id),
                usuario=usuario,
            )

            # 2) marca status por ID (garantido único)
            marcar = 'Pago' if pagar >= (valor_doc - 0.005) else 'Parcial'
            cur.execute("""
                UPDATE contas_a_pagar_mov
                   SET status = ?
                 WHERE id = ?
            """, (marcar, int(row_id)))

            restante = round(restante - pagar, 2)

        return restante

    # ---------- DINHEIRO ----------
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
        pagamento_destino: Optional[str] = None
    ) -> Tuple[int, int]:
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if origem_dinheiro not in ("Caixa", "Caixa 2"):
            raise ValueError("Origem do dinheiro inválida (use 'Caixa' ou 'Caixa 2').")

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

            # (1) INSERT saída
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
            obs = f"Saída {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'saida', ?, 'saidas', ?, 'saida', ?, ?)
            """, (data, origem_dinheiro, float(valor), obs, id_saida, trans_uid))
            id_mov = int(cur.lastrowid)

            # (4) Fluxo antigo (opcional)
            if vinculo_pagamento:
                self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id = int(vinculo_pagamento["obrigacao_id"]),
                    tipo_obrigacao = str(vinculo_pagamento["tipo_obrigacao"]),
                    valor_pago = float(vinculo_pagamento.get("valor_pagar", vinculo_pagamento.get("valor_pago", valor))),
                    data_evento = data,
                    forma_pagamento = "DINHEIRO",
                    origem = origem_dinheiro,
                    ledger_id = id_saida,
                    usuario = usuario
                )

            # (5) Classificação e auto-baixa
            if pagamento_tipo and pagamento_destino:
                self._classificar_conta_a_pagar_por_destino(conn, pagamento_tipo, pagamento_destino)
                _ = self._auto_baixar_pagamentos(
                    conn,
                    pagamento_tipo=pagamento_tipo,
                    pagamento_destino=pagamento_destino,
                    valor_total=float(valor),
                    data_evento=data,
                    forma_pagamento="DINHEIRO",
                    origem=origem_dinheiro,
                    ledger_id=id_saida,
                    usuario=usuario
                )

            conn.commit()
            return (id_saida, id_mov)

    # ---------- PIX / DÉBITO ----------
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
        pagamento_destino: Optional[str] = None
    ) -> Tuple[int, int]:
        forma_u = sanitize(forma).upper()
        if forma_u == "DEBITO":
            forma_u = "DÉBITO"
        if forma_u not in ("PIX", "DÉBITO"):
            raise ValueError("Forma inválida para saída bancária.")
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

            # (1) INSERT saída
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
            obs = f"Saída {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'saida', ?, 'saidas', ?, 'saida', ?, ?)
            """, (data, banco_nome, float(valor), obs, id_saida, trans_uid))
            id_mov = int(cur.lastrowid)

            # (4) Fluxo antigo (opcional)
            if vinculo_pagamento:
                self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id = int(vinculo_pagamento["obrigacao_id"]),
                    tipo_obrigacao = str(vinculo_pagamento["tipo_obrigacao"]),
                    valor_pago = float(vinculo_pagamento.get("valor_pagar", vinculo_pagamento.get("valor_pago", valor))),
                    data_evento = data,
                    forma_pagamento = forma_u,
                    origem = banco_nome,
                    ledger_id = id_saida,
                    usuario = usuario
                )

            # (5) Classificação e auto-baixa
            if pagamento_tipo and pagamento_destino:
                self._classificar_conta_a_pagar_por_destino(conn, pagamento_tipo, pagamento_destino)
                _ = self._auto_baixar_pagamentos(
                    conn,
                    pagamento_tipo=pagamento_tipo,
                    pagamento_destino=pagamento_destino,
                    valor_total=float(valor),
                    data_evento=data,
                    forma_pagamento=forma_u,
                    origem=banco_nome,
                    ledger_id=id_saida,
                    usuario=usuario
                )

            conn.commit()
            return (id_saida, id_mov)

    # ---------- CRÉDITO (programado em fatura) ----------
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
        fechamento: int,
        vencimento: int,
        trans_uid: Optional[str] = None
    ) -> Tuple[List[int], int]:
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if int(parcelas) < 1:
            raise ValueError("Quantidade de parcelas inválida.")

        cartao_nome   = sanitize(cartao_nome)
        categoria     = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao     = sanitize(descricao)
        usuario       = sanitize(usuario)

        trans_uid = trans_uid or uid_credito_programado(
            data_compra, valor, parcelas, cartao_nome, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return ([], -1)

        compra = pd.to_datetime(data_compra)
        base = (compra + pd.DateOffset(months=1)) if compra.day > int(fechamento) else compra
        valor_parc = round(float(valor) / int(parcelas), 2)
        ajuste = round(float(valor) - valor_parc * int(parcelas), 2)

        ids_mov_cap: List[int] = []
        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            base_obrig_id = self.cap_repo.proximo_obrigacao_id(conn)

            for p in range(1, int(parcelas) + 1):
                vcto = base.replace(day=int(vencimento)) + pd.DateOffset(months=p-1)
                vparc = valor_parc + (ajuste if p == int(parcelas) else 0.0)

                obrigacao_id = base_obrig_id + (p - 1)
                lanc_id = self.cap_repo.registrar_lancamento(
                    conn,
                    obrigacao_id=obrigacao_id,
                    tipo_obrigacao="FATURA_CARTAO",
                    valor_total=float(vparc),
                    data_evento=str(compra.date()),
                    vencimento=str(vcto.date()),
                    descricao=descricao or f"{cartao_nome} {p}/{int(parcelas)} - {categoria}/{sub_categoria}",
                    credor=cartao_nome,
                    competencia=str(vcto.date())[:7],
                    parcela_num=p,
                    parcelas_total=int(parcelas),
                    usuario=usuario
                )
                ids_mov_cap.append(int(lanc_id))

            # marca origem/cartão + status 'Em aberto' nas novas parcelas
            cur.execute("""
                UPDATE contas_a_pagar_mov
                   SET tipo_origem='FATURA_CARTAO',
                       cartao_id = (SELECT id FROM cartoes_credito WHERE LOWER(TRIM(nome)) = LOWER(TRIM(?)) LIMIT 1),
                       status = COALESCE(NULLIF(status,''), 'Em aberto')
                 WHERE obrigacao_id BETWEEN ? AND ?
            """, (cartao_nome, base_obrig_id, base_obrig_id + int(parcelas) - 1))

            obs = f"Despesa CRÉDITO {cartao_nome} {parcelas}x - {categoria}/{sub_categoria}"
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'saida', ?, 'saidas_credito_programada', ?, 'contas_a_pagar_mov', ?, ?)
            """, (str(compra.date()), cartao_nome, float(valor), obs, ids_mov_cap[0] if ids_mov_cap else None, trans_uid))
            id_mov = int(cur.lastrowid)

            conn.commit()
            return (ids_mov_cap, id_mov)

    # ---------- BOLETO (programado) ----------
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
            raise ValueError("Quantidade de parcelas inválida.")

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

            # marca origem + status 'Em aberto' nas novas parcelas
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