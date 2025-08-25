"""
service_ledger_credito.py — Compras a crédito (programação em fatura).

Resumo:
    Regras para agregar compras a crédito na fatura do cartão:
    - Cria/atualiza o LANCAMENTO da fatura (por competência).
    - Rateia valor em N parcelas, respeitando fechamento/vencimento do cartão.
    - Loga programação em movimentacoes_bancarias e itens em fatura_cartao_itens.

Responsabilidades:
    - Determinar competência base da compra conforme regras do cartão.
    - Criar/atualizar LANCAMENTO da fatura (tipo_obrigacao='FATURA_CARTAO').
    - Inserir itens detalhados em fatura_cartao_itens.
    - Preservar idempotência via trans_uid (mov_repo.ja_existe_transacao).

Depende de:
    - shared.db.get_conn
    - shared.ids.sanitize, uid_credito_programado
    - self.cap_repo (proximo_obrigacao_id, registrar_lancamento)
    - self.mov_repo (ja_existe_transacao)
    - self.cartoes_repo (obter_por_nome)
    - self._competencia_compra (helper na service/fachada)

Notas de segurança:
    - SQL apenas com parâmetros (?); sem interpolar dados do usuário.
    - _expr_valor_documento é constante/segura quando usada em consultas correlatas.
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Imports + bootstrap de caminho (robusto em execuções via Streamlit)
# -----------------------------------------------------------------------------
import logging
import calendar
from datetime import datetime
from typing import Optional

import os
import sys

# Garante que a raiz do projeto (<raiz>/services/ledger/..) esteja no sys.path
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Terceiros
import pandas as pd  # noqa: E402

# Internos
from shared.db import get_conn  # noqa: E402
from shared.ids import sanitize, uid_credito_programado  # noqa: E402

logger = logging.getLogger(__name__)

__all__ = ["_CreditoLedgerMixin"]


class _CreditoLedgerMixin:
    """Mixin de regras para compras a crédito (programadas em fatura)."""

    # ----------------------------------------------------------------------
    # Atualiza (ou cria) o LANCAMENTO da fatura de uma competência
    # ----------------------------------------------------------------------
    def _add_valor_fatura(
        self,
        conn,
        *,
        cartao_nome: str,
        competencia: str,
        valor_add: float,
        data_evento: str,
        vencimento: str,
        usuario: str,
        descricao: str | None,
        parcela_num: int | None = None,
        parcelas_total: int | None = None,
    ) -> int:
        """
        Soma `valor_add` ao LANCAMENTO da fatura (cartão+competência). Se não existir,
        cria o LANCAMENTO com status 'Em aberto' e marca tipo_origem='FATURA_CARTAO'.

        Retorna:
            int: id do LANCAMENTO (contas_a_pagar_mov.id)
        """
        cur = conn.cursor()

        row = cur.execute(
            """
            SELECT id, obrigacao_id, COALESCE(valor_evento,0.0) AS valor_atual
              FROM contas_a_pagar_mov
             WHERE tipo_obrigacao='FATURA_CARTAO'
               AND categoria_evento='LANCAMENTO'
               AND LOWER(TRIM(credor)) = LOWER(TRIM(?))
               AND competencia = ?
             LIMIT 1
            """,
            (cartao_nome, competencia),
        ).fetchone()

        if row:
            lanc_id = int(row[0])
            obrigacao_id = int(row[1])
            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET valor_evento = COALESCE(valor_evento,0) + ?,
                       descricao    = COALESCE(descricao, ?)
                 WHERE id = ?
                """,
                (float(valor_add), descricao, lanc_id),
            )
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
                usuario=usuario,
            )
            # marca origem/status/cartao_id
            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET tipo_origem='FATURA_CARTAO',
                       cartao_id = (SELECT id FROM cartoes_credito
                                      WHERE LOWER(TRIM(nome)) = LOWER(TRIM(?)) LIMIT 1),
                       status = COALESCE(NULLIF(status,''), 'Em aberto')
                 WHERE id = ?
                """,
                (cartao_nome, lanc_id),
            )

        # garantir status coerente após alteração
        row2 = cur.execute(
            "SELECT id, obrigacao_id, COALESCE(valor_evento,0) FROM contas_a_pagar_mov WHERE id=?",
            (lanc_id,),
        ).fetchone()
        obrigacao_id = int(row2[1])
        valor_doc = float(row2[2])
        self._atualizar_status_por_id(conn, lanc_id, obrigacao_id, valor_doc)

        logger.debug(
            "_add_valor_fatura: cartao=%s comp=%s add=%.2f lanc_id=%s obrig=%s",
            cartao_nome, competencia, valor_add, lanc_id, obrigacao_id
        )
        return lanc_id

    # ----------------------------------------------------------------------
    # Programa compra a crédito em N parcelas na(s) fatura(s)
    # ----------------------------------------------------------------------
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
        fechamento: int,  # (ignorado; usa dados do cartão no banco)
        vencimento: int,  # (ignorado; usa dados do cartão no banco)
        trans_uid: Optional[str] = None,
    ) -> tuple[list[int], int]:
        """
        Rateia `valor` em `parcelas` e agrega cada parcela à fatura adequada
        (competências sucessivas), conforme regras do cartão (fechamento/vencimento).

        Retorna:
            (lista_ids_lancamentos_fatura, id_movimentacao_bancaria_programacao)

        Observações:
            - Idempotência: se trans_uid já existir -> retorna ([], -1).
            - Usa cartoes_repo.obter_por_nome(cartao_nome) -> (vencimento_dia, dias_fechamento).
        """
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if int(parcelas) < 1:
            raise ValueError("Quantidade de parcelas inválida.")

        cartao_nome = sanitize(cartao_nome)
        categoria = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao = sanitize(descricao)
        usuario = sanitize(usuario)

        trans_uid = trans_uid or uid_credito_programado(
            data_compra, valor, parcelas, cartao_nome, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            logger.info("registrar_saida_credito: trans_uid já existe (%s) — ignorando", trans_uid)
            return ([], -1)

        compra = pd.to_datetime(data_compra)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # pega configuração do cartão
            card_cfg = self.cartoes_repo.obter_por_nome(cartao_nome)
            if not card_cfg:
                raise ValueError(f"Cartão '{cartao_nome}' não encontrado.")
            try:
                vencimento_dia, dias_fechamento = card_cfg
            except Exception:
                raise ValueError(
                    f"cartoes_repo.obter_por_nome('{cartao_nome}') deve retornar (vencimento_dia, dias_fechamento)."
                )

            # competência base da compra conforme regra (helper da service/fachada)
            comp_base_str = self._competencia_compra(
                compra_dt=pd.to_datetime(compra).to_pydatetime(),
                vencimento_dia=int(vencimento_dia),
                dias_fechamento=int(dias_fechamento),
            )
            comp_base = pd.to_datetime(comp_base_str + "-01")

            # Rateio com ajuste na última parcela (evita sobra de centavos)
            valor_parc = round(float(valor) / int(parcelas), 2)
            ajuste = round(float(valor) - valor_parc * int(parcelas), 2)

            lanc_ids: list[int] = []
            total_programado = 0.0

            for p in range(1, int(parcelas) + 1):
                comp_dt = comp_base + pd.DateOffset(months=p - 1)
                y, m = comp_dt.year, comp_dt.month

                last = calendar.monthrange(y, m)[1]
                venc_d = min(int(vencimento_dia), last)
                vcto_date = datetime(y, m, venc_d).date()
                competencia = f"{y:04d}-{m:02d}"

                vparc = round(valor_parc + (ajuste if p == int(parcelas) else 0.0), 2)

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
                    parcelas_total=int(parcelas),
                )
                lanc_ids.append(int(lanc_id))
                total_programado = round(total_programado + float(vparc), 2)

                # item da compra, útil para auditoria da fatura
                cur.execute(
                    """
                    INSERT INTO fatura_cartao_itens
                        (purchase_uid, cartao, competencia, data_compra, descricao_compra, categoria,
                         parcela_num, parcelas, valor_parcela, usuario)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trans_uid,
                        cartao_nome,
                        competencia,
                        str(compra.date()),
                        descricao or "",
                        (f"{categoria or ''}" + (f" / {sub_categoria}" if sub_categoria else "")).strip(" /"),
                        int(p),
                        int(parcelas),
                        float(vparc),
                        usuario,
                    ),
                )

            obs = f"Despesa CREDITO {cartao_nome} {parcelas}x - {categoria}/{sub_categoria}"
            cur.execute(
                """
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'saida', ?, 'saidas_credito_programada', ?, 'contas_a_pagar_mov', ?, ?)
                """,
                (
                    str(compra.date()),
                    cartao_nome,
                    float(total_programado),
                    obs,
                    lanc_ids[0] if lanc_ids else None,
                    trans_uid,
                ),
            )
            id_mov = int(cur.lastrowid)

            conn.commit()

        logger.debug(
            "registrar_saida_credito: cartao=%s parcelas=%s total=%.2f comp_base=%s lanc_ids=%s mov=%s",
            cartao_nome, parcelas, total_programado, comp_base_str, lanc_ids, id_mov
        )
        return (lanc_ids, id_mov)
