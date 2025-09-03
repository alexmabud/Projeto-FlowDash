"""
Compras a crédito (programação em fatura).

Regras para agregar compras a crédito na fatura do cartão:
- Cria/atualiza o LANCAMENTO da fatura (por competência).
- Rateia valor em N parcelas, respeitando fechamento/vencimento do cartão.
- Loga a programação em `movimentacoes_bancarias` e itens em `fatura_cartao_itens`.

Responsabilidades:
- Determinar a competência base da compra conforme regras do cartão.
- Criar/atualizar LANCAMENTO da fatura (`tipo_obrigacao='FATURA_CARTAO'`).
- Inserir itens detalhados em `fatura_cartao_itens`.
- Preservar idempotência via `trans_uid` (`mov_repo.ja_existe_transacao`).

Dependências:
- shared.db.get_conn
- shared.ids.sanitize, uid_credito_programado
- self.cap_repo (proximo_obrigacao_id, registrar_lancamento)
- self.mov_repo (ja_existe_transacao)
- self.cartoes_repo (obter_por_nome)
- self._competencia_compra (helper na service/fachada)

Notas:
- NÃO atualizamos coluna legada `valor`.
- Status: "EM ABERTO" / "PARCIAL" / "QUITADO" (definido pelo helper).
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import calendar
import logging
import os
import sys
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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
from services.ledger.service_ledger_infra import (  # noqa: E402
    _fmt_obs_saida,
    log_mov_bancaria,
)

logger = logging.getLogger(__name__)

__all__ = ["_CreditoLedgerMixin"]

_EPS = 0.005  # tolerância monetária para arredondamentos


class _CreditoLedgerMixin:
    """Mixin de regras para compras a crédito (programadas em fatura).

    Este mixin agrega compras a crédito às respectivas faturas de cartão, criando
    (ou atualizando) um LANCAMENTO por competência e registrando itens
    descritivos em `fatura_cartao_itens`. A auditoria fica em `movimentacoes_bancarias`.
    """

    # ------------------------------------------------------------------
    # Atualiza (ou cria) o LANCAMENTO da fatura de uma competência
    # ------------------------------------------------------------------
    def _add_valor_fatura(
        self,
        conn: sqlite3.Connection,
        *,
        cartao_nome: str,
        competencia: str,
        valor_add: float,
        data_evento: str,
        vencimento: str,
        usuario: str,
        descricao: Optional[str],
        parcela_num: Optional[int] = None,
        parcelas_total: Optional[int] = None,
    ) -> int:
        """Soma `valor_add` ao LANCAMENTO da fatura (cartão+competência).

        Se já houver LANCAMENTO para (cartão, competência), acumula em `valor_evento`.
        Caso contrário, cria um novo LANCAMENTO.

        Observação:
            O UPDATE usa `descricao = COALESCE(descricao, ?)` para preservar uma
            descrição já existente; só preenche se estiver NULL.

        Args:
            conn: Conexão SQLite aberta.
            cartao_nome: Nome do cartão (credor no CAP).
            competencia: Competência no formato `YYYY-MM`.
            valor_add: Valor a somar ao principal da fatura.
            data_evento: Data do evento (normalmente a data da compra).
            vencimento: Data de vencimento calculada para a competência.
            usuario: Operador responsável.
            descricao: Descrição genérica para o LANCAMENTO (pode ser None).
            parcela_num: Número da parcela (para sinalizar no CAP).
            parcelas_total: Total de parcelas.

        Returns:
            int: ID do LANCAMENTO (contas_a_pagar_mov.id).
        """
        cur = conn.cursor()
        valor_add = round(float(valor_add or 0.0), 2)

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
            lanc_id = int(row["id"])
            _ = int(row["obrigacao_id"])  # mantido para debug/log
            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET valor_evento = COALESCE(valor_evento,0) + ?,
                       descricao    = COALESCE(descricao, ?)
                 WHERE id = ?
                """,
                (valor_add, descricao, lanc_id),
            )
        else:
            obrigacao_id = self.cap_repo.proximo_obrigacao_id(conn)  # type: ignore[attr-defined]
            lanc_id = self.cap_repo.registrar_lancamento(  # type: ignore[attr-defined]
                conn,
                obrigacao_id=int(obrigacao_id),
                tipo_obrigacao="FATURA_CARTAO",
                valor_total=valor_add,
                data_evento=data_evento,
                vencimento=vencimento,
                descricao=descricao or f"Fatura {cartao_nome} {competencia}",
                credor=cartao_nome,
                competencia=competencia,
                parcela_num=int(parcela_num) if parcela_num is not None else 1,
                parcelas_total=int(parcelas_total) if parcelas_total is not None else 1,
                usuario=usuario,
            )
            # Marca origem/status/cartao_id (status em MAIÚSCULAS quando vazio)
            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET tipo_origem='FATURA_CARTAO',
                       cartao_id = (SELECT id FROM cartoes_credito
                                      WHERE LOWER(TRIM(nome)) = LOWER(TRIM(?)) LIMIT 1),
                       status = COALESCE(NULLIF(status,''), 'EM ABERTO')
                 WHERE id = ?
                """,
                (cartao_nome, lanc_id),
            )

        # Recalcula STATUS de forma segura (helper busca os números no BD)
        self._atualizar_status_por_id(conn, int(lanc_id))  # type: ignore[attr-defined]

        logger.debug(
            "_add_valor_fatura: cartao=%s comp=%s add=%.2f lanc_id=%s",
            cartao_nome, competencia, valor_add, lanc_id,
        )
        return int(lanc_id)

    # ------------------------------------------------------------------
    # Programa compra a crédito em N parcelas na(s) fatura(s)
    # ------------------------------------------------------------------
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
        fechamento: int,  # (IGNORADO — usa dados do cartão no banco; mantido por compat)
        vencimento: int,  # (IGNORADO — usa dados do cartão no banco; mantido por compat)
        trans_uid: Optional[str] = None,
    ) -> Tuple[List[int], int]:
        """Rateia o valor em parcelas e agrega cada parcela à fatura adequada.

        Idempotência: se `trans_uid` já existe em `movimentacoes_bancarias`,
        a operação é ignorada.

        Args:
            data_compra: Data da compra (YYYY-MM-DD).
            valor: Valor total da compra (> 0).
            parcelas: Número de parcelas (>= 1).
            cartao_nome: Nome do cartão.
            categoria: Categoria principal (para item).
            sub_categoria: Subcategoria (para item).
            descricao: Descrição detalhada (para item).
            usuario: Identificação do operador.
            fechamento: (Ignorado) mantido para compatibilidade de chamadas antigas.
            vencimento: (Ignorado) mantido para compatibilidade de chamadas antigas.
            trans_uid: UID determinístico opcional.

        Returns:
            Tuple[List[int], int]: (IDs de LANCAMENTO afetados/criados, id do log em `movimentacoes_bancarias`)
            ou ([], -1) se idempotente.
        """
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if int(parcelas) < 1:
            raise ValueError("Quantidade de parcelas inválida.")

        cartao_nome = sanitize(cartao_nome)
        categoria = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao_item = sanitize(descricao)  # detalhada para os itens de fatura
        usuario = sanitize(usuario)

        # Descrição genérica para o LANCAMENTO (CAP)
        descricao_cap = "Descrição na Fatura"

        # Idempotência
        trans_uid = trans_uid or uid_credito_programado(
            data_compra, valor, parcelas, cartao_nome, categoria, sub_categoria, descricao_item, usuario
        )
        try:
            if getattr(self, "mov_repo", None) and self.mov_repo.ja_existe_transacao(trans_uid):  # type: ignore[attr-defined]
                logger.info("registrar_saida_credito: trans_uid já existe (%s) — ignorando", trans_uid)
                return ([], -1)
        except Exception:
            # Falha em checar idempotência não deve travar a programação
            pass

        compra = pd.to_datetime(data_compra)

        with get_conn(self.db_path) as conn:  # type: ignore[attr-defined]
            cur = conn.cursor()

            # Config do cartão (aceita tupla (vencimento_dia, dias_fechamento) ou dict equivalente)
            card_cfg: Any = self.cartoes_repo.obter_por_nome(cartao_nome)  # type: ignore[attr-defined]
            if not card_cfg:
                raise ValueError(f"Cartão '{cartao_nome}' não encontrado.")
            try:
                if isinstance(card_cfg, dict):
                    vencimento_dia = int(card_cfg.get("vencimento_dia"))
                    dias_fechamento = int(card_cfg.get("dias_fechamento"))
                else:
                    vencimento_dia, dias_fechamento = map(int, card_cfg)  # type: ignore[arg-type]
            except Exception as e:
                raise ValueError(
                    f"cartoes_repo.obter_por_nome('{cartao_nome}') deve retornar "
                    f"(vencimento_dia, dias_fechamento) ou dict compatível."
                ) from e

            # Competência base da compra (regra do cartão)
            comp_base_str = self._competencia_compra(  # type: ignore[attr-defined]
                compra_dt=pd.to_datetime(compra).to_pydatetime(),
                vencimento_dia=int(vencimento_dia),
                dias_fechamento=int(dias_fechamento),
            )
            comp_base = pd.to_datetime(comp_base_str + "-01")

            # Rateio com ajuste na última parcela (evita sobra/defasagem de centavos)
            valor_parc = round(float(valor) / int(parcelas), 2)
            ajuste = round(float(valor) - valor_parc * int(parcelas), 2)

            lanc_ids: List[int] = []
            total_programado = 0.0

            for p in range(1, int(parcelas) + 1):
                comp_dt = comp_base + pd.DateOffset(months=p - 1)
                y, m = comp_dt.year, comp_dt.month

                last = calendar.monthrange(y, m)[1]
                venc_d = min(int(vencimento_dia), last)
                vcto_date = datetime(y, m, venc_d).date()
                competencia = f"{y:04d}-{m:02d}"

                vparc = round(valor_parc + (ajuste if p == int(parcelas) else 0.0), 2)

                # LANCAMENTO na CAP com descrição genérica (mantém/povoa via COALESCE)
                lanc_id = self._add_valor_fatura(
                    conn,
                    cartao_nome=cartao_nome,
                    competencia=competencia,
                    valor_add=float(vparc),
                    data_evento=str(compra.date()),
                    vencimento=str(vcto_date),
                    usuario=usuario,
                    descricao=descricao_cap or f"Fatura {cartao_nome} {competencia}",
                    parcela_num=p,
                    parcelas_total=int(parcelas),
                )
                lanc_ids.append(int(lanc_id))
                total_programado = round(total_programado + float(vparc), 2)

                # Item detalhado na fatura (usa descrição do formulário)
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
                        descricao_item or "",
                        (f"{categoria or ''}" + (f" / {sub_categoria}" if sub_categoria else "")).strip(" /"),
                        int(p),
                        int(parcelas),
                        float(vparc),
                        usuario,
                    ),
                )

            # Log — padronizado COM parcelas (• Nx) para crédito
            obs = _fmt_obs_saida(
                forma="CREDITO",
                valor=float(total_programado),
                categoria=categoria,
                subcategoria=sub_categoria,
                descricao=descricao_item,
                cartao=cartao_nome,
                parcelas=int(parcelas),  # anexa " • Nx" se >= 2
            )

            id_mov = log_mov_bancaria(
                conn,
                data=str(compra.date()),
                banco=cartao_nome,
                tipo="saida",
                valor=float(total_programado),
                origem="saidas_credito_programada",
                observacao=obs,
                usuario=usuario,
                referencia_tabela="contas_a_pagar_mov",
                referencia_id=(lanc_ids[0] if lanc_ids else None),
                trans_uid=trans_uid,
                data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

            conn.commit()

        logger.debug(
            "registrar_saida_credito: cartao=%s parcelas=%s total=%.2f comp_base=%s lanc_ids=%s mov=%s",
            cartao_nome, parcelas, total_programado, comp_base_str, lanc_ids, id_mov,
        )
        return (lanc_ids, id_mov)
