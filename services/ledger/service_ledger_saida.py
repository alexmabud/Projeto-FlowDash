"""
Saídas (dinheiro e bancária).

Regras para registrar saídas de dinheiro (Caixa/Caixa 2) e saídas bancárias
(PIX/DÉBITO), com logs em `movimentacoes_bancarias` e integrações com CAP:

- Vínculo direto a um título (obrigacao_id/tipo_obrigacao).
- Auto-classificação por destino (cartão/boletos/empréstimos).
- Auto-baixa de pagamentos.
- Atalho de pagamento direto de fatura por `obrigacao_id_fatura`.

Dependências:
- shared.db.get_conn
- shared.ids.sanitize, uid_saida_dinheiro, uid_saida_bancaria
- self.mov_repo.ja_existe_transacao
- self.cap_repo (registrar_pagamento, obter_saldo_obrigacao)
- Mixins auxiliares:
  - _garantir_linha_saldos_caixas/_bancos
  - _ajustar_banco_dynamic
  - _classificar_conta_a_pagar_por_destino
  - _auto_baixar_pagamentos
  - _pagar_fatura_por_obrigacao

Notas de segurança:
- SQL apenas com parâmetros (?).
- Atualização do saldo de caixas com coluna **whitelist** (evita injeção).
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Optional, Tuple

# Garante que a raiz do projeto (<raiz>/services/ledger/..) esteja no sys.path
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Internos
from shared.db import get_conn  # noqa: E402
from shared.ids import sanitize, uid_saida_bancaria, uid_saida_dinheiro  # noqa: E402
from services.ledger.service_ledger_infra import (  # noqa: E402
    _ensure_mov_cols,
    _fmt_obs_saida,
)

logger = logging.getLogger(__name__)

__all__ = ["_SaidasLedgerMixin"]


class _SaidasLedgerMixin:
    """Regras de saída (dinheiro e bancária)."""

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
        """Registra uma saída em dinheiro (Caixa/Caixa 2) e integra com CAP (opcional).

        Valida o valor, ajusta o saldo do caixa correspondente, grava log em
        `movimentacoes_bancarias` e, quando aplicável, executa:
        - pagamento direto de fatura via `obrigacao_id_fatura`;
        - vínculo direto com um título via `vinculo_pagamento`;
        - classificação e auto-baixa por destino (fatura/boletos/empréstimos).

        Args:
            data (str): Data do lançamento em 'YYYY-MM-DD'.
            valor (float): Valor da saída (> 0).
            origem_dinheiro (str): "Caixa" ou "Caixa 2".
            categoria (Optional[str]): Categoria da saída.
            sub_categoria (Optional[str]): Subcategoria da saída.
            descricao (Optional[str]): Descrição livre do lançamento.
            usuario (str): Nome do usuário logado que executa o lançamento.
            trans_uid (Optional[str]): UID idempotente da transação (auto-gerado se None).
            vinculo_pagamento (Optional[Dict]): Vínculo direto com título (obrigacao_id/tipo_obrigacao/valor).
            pagamento_tipo (Optional[str]): Tipo de pagamento para classificação (ex.: "FATURA_CARTAO").
            pagamento_destino (Optional[str]): Destino para classificação (ex.: nome do cartão).
            competencia_pagamento (Optional[str]): Competência contábil do pagamento (ex.: "2025-08").
            obrigacao_id_fatura (Optional[int]): Id de obrigação para pagar fatura diretamente.

        Returns:
            Tuple[int, int]: `(id_saida, id_mov_bancaria)`; retorna `(-1, -1)` se já existir `trans_uid`.

        Raises:
            ValueError: Se `valor <= 0` ou `origem_dinheiro` inválida.
        """
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if origem_dinheiro not in ("Caixa", "Caixa 2"):
            raise ValueError("Origem do dinheiro inválida (use 'Caixa' ou 'Caixa 2').")

        categoria = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao = sanitize(descricao)
        usuario = sanitize(usuario)

        trans_uid = trans_uid or uid_saida_dinheiro(
            data, valor, origem_dinheiro, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            logger.info(
                "registrar_saida_dinheiro: trans_uid já existe (%s) — ignorando",
                trans_uid,
            )
            return (-1, -1)

        eps = 0.005

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()
            self._garantir_linha_saldos_caixas(conn, data)

            # (1) INSERT saida
            cur.execute(
                """
                INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                   Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                   Origem_Dinheiro, Banco_Saida)
                VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
                """,
                (data, categoria, sub_categoria, descricao, float(valor), usuario, origem_dinheiro),
            )
            id_saida = int(cur.lastrowid)

            # (2) Ajusta saldos de caixa com coluna validada (whitelist)
            col_map = {"Caixa": "caixa", "Caixa 2": "caixa_2"}
            col = col_map.get(origem_dinheiro)
            cur.execute(
                f"UPDATE saldos_caixas SET {col} = COALESCE({col},0) - ? WHERE data = ?",
                (float(valor), data),
            )

            # (3) Log movimentação bancária
            _ensure_mov_cols(cur)
            obs = _fmt_obs_saida(
                forma="DINHEIRO",
                valor=float(valor),
                categoria=categoria,
                subcategoria=sub_categoria,
                descricao=descricao,
            )
            cur.execute(
                """
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao,
                     referencia_tabela, referencia_id, trans_uid, usuario, data_hora)
                VALUES (?, ?, 'saida', ?, 'saidas', ?, 'saida', ?, ?, ?, ?)
                """,
                (
                    data,
                    origem_dinheiro,
                    float(valor),
                    obs,
                    id_saida,
                    trans_uid,
                    usuario,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            id_mov = int(cur.lastrowid)

            # (3.1) Pagamento direto de fatura (prioritário)
            if obrigacao_id_fatura:
                sobra = self._pagar_fatura_por_obrigacao(
                    conn,
                    obrigacao_id=int(obrigacao_id_fatura),
                    valor=float(valor),
                    data_evento=data,
                    forma_pagamento="DINHEIRO",
                    origem=origem_dinheiro,
                    ledger_id=id_saida,
                    usuario=usuario,
                )
                if sobra > eps:
                    cur.execute(
                        """
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                        """,
                        (float(sobra), id_mov),
                    )
                conn.commit()
                logger.debug(
                    "registrar_saida_dinheiro: id_saida=%s id_mov=%s sobra=%.2f",
                    id_saida,
                    id_mov,
                    sobra,
                )
                return (id_saida, id_mov)

            # (4) Vínculo direto com um título (opcional)
            if vinculo_pagamento:
                obrig_id = int(vinculo_pagamento["obrigacao_id"])
                tipo_obrig = str(vinculo_pagamento["tipo_obrigacao"])
                val = float(
                    vinculo_pagamento.get(
                        "valor_pagar",
                        vinculo_pagamento.get("valor_pago", valor),
                    )
                )

                self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id=obrig_id,
                    tipo_obrigacao=tipo_obrig,
                    valor_pago=val,
                    data_evento=data,
                    forma_pagamento="DINHEIRO",
                    origem=origem_dinheiro,
                    ledger_id=id_saida,
                    usuario=usuario,
                )
                self._atualizar_status_por_obrigacao(conn, obrig_id)

            # (5) Classificação + Auto-baixa por destino/tipo
            if pagamento_tipo and pagamento_destino:
                self._classificar_conta_a_pagar_por_destino(
                    conn, pagamento_tipo, pagamento_destino
                )
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
                    competencia_pagamento=competencia_pagamento,
                )
                if restante > eps:
                    cur.execute(
                        """
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                        """,
                        (float(restante), id_mov),
                    )

            conn.commit()
            logger.debug("registrar_saida_dinheiro: id_saida=%s id_mov=%s", id_saida, id_mov)
            return (id_saida, id_mov)

    def registrar_saida_bancaria(
        self,
        *,
        data: str,
        valor: float,
        banco_nome: str,
        forma: str,  # "PIX" ou "DÉBITO"
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
        """Registra uma saída bancária (PIX/DÉBITO) e integra com CAP (opcional).

        Valida o valor, ajusta o saldo do banco correspondente, grava log em
        `movimentacoes_bancarias` e, quando aplicável, executa:
        - pagamento direto de fatura via `obrigacao_id_fatura`;
        - vínculo direto com um título via `vinculo_pagamento`;
        - classificação e auto-baixa por destino (fatura/boletos/empréstimos).

        Args:
            data (str): Data do lançamento em 'YYYY-MM-DD'.
            valor (float): Valor da saída (> 0).
            banco_nome (str): Nome da coluna de banco a ser ajustada.
            forma (str): "PIX" ou "DÉBITO".
            categoria (Optional[str]): Categoria da saída.
            sub_categoria (Optional[str]): Subcategoria da saída.
            descricao (Optional[str]): Descrição livre do lançamento.
            usuario (str): Nome do usuário logado que executa o lançamento.
            trans_uid (Optional[str]): UID idempotente da transação (auto-gerado se None).
            vinculo_pagamento (Optional[Dict]): Vínculo direto com título (obrigacao_id/tipo_obrigacao/valor).
            pagamento_tipo (Optional[str]): Tipo de pagamento para classificação (ex.: "FATURA_CARTAO").
            pagamento_destino (Optional[str]): Destino para classificação (ex.: nome do cartão).
            competencia_pagamento (Optional[str]): Competência contábil do pagamento (ex.: "2025-08").
            obrigacao_id_fatura (Optional[int]): Id de obrigação para pagar fatura diretamente.

        Returns:
            Tuple[int, int]: `(id_saida, id_mov_bancaria)`; retorna `(-1, -1)` se já existir `trans_uid`.

        Raises:
            ValueError: Se `valor <= 0` ou `forma` inválida.
        """
        forma_u = sanitize(forma).upper()
        if forma_u == "DEBITO":
            forma_u = "DÉBITO"
        if forma_u not in ("PIX", "DÉBITO"):
            raise ValueError("Forma inválida para saída bancária.")
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")

        banco_nome = sanitize(banco_nome)
        categoria = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao = sanitize(descricao)
        usuario = sanitize(usuario)

        trans_uid = trans_uid or uid_saida_bancaria(
            data, valor, banco_nome, forma_u, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            logger.info(
                "registrar_saida_bancaria: trans_uid já existe (%s) — ignorando",
                trans_uid,
            )
            return (-1, -1)

        eps = 0.005

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # (1) INSERT saida
            cur.execute(
                """
                INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                   Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                   Origem_Dinheiro, Banco_Saida)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
                """,
                (data, categoria, sub_categoria, descricao, forma_u, float(valor), usuario, banco_nome),
            )
            id_saida = int(cur.lastrowid)

            # (2) Ajusta saldos de bancos
            self._garantir_linha_saldos_bancos(conn, data)
            self._ajustar_banco_dynamic(conn, banco_col=banco_nome, delta=-float(valor), data=data)

            # (3) Log movimentação bancária
            _ensure_mov_cols(cur)
            obs = _fmt_obs_saida(
                forma=forma_u,
                valor=float(valor),
                categoria=categoria,
                subcategoria=sub_categoria,
                descricao=descricao,
                banco=(banco_nome if forma_u == "DÉBITO" else None),  # PIX não exibe nome do banco
            )
            cur.execute(
                """
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao,
                     referencia_tabela, referencia_id, trans_uid, usuario, data_hora)
                VALUES (?, ?, 'saida', ?, 'saidas', ?, 'saida', ?, ?, ?, ?)
                """,
                (
                    data,
                    banco_nome,
                    float(valor),
                    obs,
                    id_saida,
                    trans_uid,
                    usuario,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            id_mov = int(cur.lastrowid)

            # (3.1) Pagamento direto de fatura (prioritário)
            if obrigacao_id_fatura:
                sobra = self._pagar_fatura_por_obrigacao(
                    conn,
                    obrigacao_id=int(obrigacao_id_fatura),
                    valor=float(valor),
                    data_evento=data,
                    forma_pagamento=forma_u,
                    origem=banco_nome,
                    ledger_id=id_saida,
                    usuario=usuario,
                )
                if sobra > eps:
                    cur.execute(
                        """
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                        """,
                        (float(sobra), id_mov),
                    )
                conn.commit()
                logger.debug(
                    "registrar_saida_bancaria: id_saida=%s id_mov=%s sobra=%.2f",
                    id_saida,
                    id_mov,
                    sobra,
                )
                return (id_saida, id_mov)

            # (4) Vínculo direto com um título (opcional)
            if vinculo_pagamento:
                obrig_id = int(vinculo_pagamento["obrigacao_id"])
                tipo_obrig = str(vinculo_pagamento["tipo_obrigacao"])
                val = float(
                    vinculo_pagamento.get(
                        "valor_pagar",
                        vinculo_pagamento.get("valor_pago", valor),
                    )
                )

                self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id=obrig_id,
                    tipo_obrigacao=tipo_obrig,
                    valor_pago=val,
                    data_evento=data,
                    forma_pagamento=forma_u,
                    origem=banco_nome,
                    ledger_id=id_saida,
                    usuario=usuario,
                )
                self._atualizar_status_por_obrigacao(conn, obrig_id)

            # (5) Classificação + Auto-baixa por destino/tipo
            if pagamento_tipo and pagamento_destino:
                self._classificar_conta_a_pagar_por_destino(
                    conn, pagamento_tipo, pagamento_destino
                )
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
                    competencia_pagamento=competencia_pagamento,
                )
                if restante > eps:
                    cur.execute(
                        """
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                        """,
                        (float(restante), id_mov),
                    )

            conn.commit()
            logger.debug("registrar_saida_bancaria: id_saida=%s id_mov=%s", id_saida, id_mov)
            return (id_saida, id_mov)
