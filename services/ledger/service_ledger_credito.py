
"""Crédito (programação em fatura) (_CreditoLedgerMixin)."""
from __future__ import annotations
import calendar
from datetime import datetime
import pandas as pd
from typing import Optional, Tuple, List
from shared.db import get_conn
from shared.ids import sanitize, uid_credito_programado

class _CreditoLedgerMixin:
    """Mixin de regras para compras a crédito (programadas em fatura)."""

    def _add_valor_fatura(self, conn, *, cartao_nome: str, competencia: str,
                          valor_add: float, data_evento: str, vencimento: str,
                          usuario: str, descricao: str | None,
                          parcela_num: int | None = None,
                          parcelas_total: int | None = None) -> int:
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
    ) -> tuple[list[int], int]:
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if int(parcelas) < 1:
            raise ValueError("Quantidade de parcelas invalida.")

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

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            vencimento_dia, dias_fechamento = self.cartoes_repo.obter_por_nome(cartao_nome)

            comp_base_str = self._competencia_compra(
                compra_dt=pd.to_datetime(compra).to_pydatetime(),
                vencimento_dia=vencimento_dia,
                dias_fechamento=dias_fechamento
            )
            comp_base = pd.to_datetime(comp_base_str + "-01")

            valor_parc = round(float(valor) / int(parcelas), 2)
            ajuste = round(float(valor) - valor_parc * int(parcelas), 2)

            lanc_ids: list[int] = []
            total_programado = 0.0

            for p in range(1, int(parcelas) + 1):
                comp_dt = (comp_base + pd.DateOffset(months=p-1))
                y, m = comp_dt.year, comp_dt.month
                last = calendar.monthrange(y, m)[1]
                venc_d = min(int(vencimento_dia), last)
                vcto_date = datetime(y, m, venc_d).date()
                competencia = f"{y:04d}-{m:02d}"

                vparc = valor_parc + (ajuste if p == int(parcelas) else 0.0)

                lanc_id = self._add_valor_fatura(
                    conn,
                    cartao_nome=cartao_nome,
                    competencia=competencia,
                    valor_add=float(vparc),
                    data_evento=str(compra.date()),
                    vencimento=str(vcto_date),
                    usuario=usuario,
                    descricao=descricao or f"Fatura {cartao_nome} {competencia}",
                    parcela_num=p,
                    parcelas_total=int(parcelas)
                )
                lanc_ids.append(int(lanc_id))
                total_programado += float(vparc)

                cur.execute("""
                    INSERT INTO fatura_cartao_itens
                        (purchase_uid, cartao, competencia, data_compra, descricao_compra, categoria,
                        parcela_num, parcelas, valor_parcela, usuario)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trans_uid,
                    cartao_nome,
                    competencia,
                    str(compra.date()),
                    descricao or "",
                    (f"{categoria or ''}" + (f" / {sub_categoria}" if sub_categoria else "")).strip(" /"),
                    int(p),
                    int(parcelas),
                    float(vparc),
                    usuario
                ))

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
