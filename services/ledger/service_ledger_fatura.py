# services/ledger/service_ledger_fatura.py
"""
Service: FATURA_CARTAO (pagamento de parcelas de fatura de cartão)

Regras principais:
- Operamos sobre um `obrigacao_id` (a fatura do mês).
- Ordem de aplicação: FIFO por vencimento das parcelas em aberto da obrigação.
- Rateio do pagamento:
    • principal → amortiza as parcelas (CASCADE entre parcelas).
    • juros     → aplica apenas na PRIMEIRA parcela ainda aberta (não cascateia).
    • multa     → aplica apenas na PRIMEIRA parcela ainda aberta (não cascateia).
    • desconto  → aplica apenas na PRIMEIRA parcela ainda aberta (não cascateia).
- Saída de caixa/banco: **principal + juros + multa** (desconto **não** sai do caixa), agregado.
- trans_uid: gerado por operação (idempotência).
- Este serviço atualiza CAP; a criação de movimentação:
  • se o pagamento vier do fluxo de SAÍDA (registrar_saida_*), a mov. já é registrada lá;
  • se o pagamento for direto por aqui, este serviço registra 1 linha em `movimentacoes_bancarias`
    usando `forma_pagamento` e `origem` passados.

Dependências (Repository):
- listar_parcelas_em_aberto_fifo(conn, obrigacao_id)
- aplicar_pagamento_parcela(conn, parcela_id, valor_base, juros, multa, desconto, data_evento, usuario)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional
from uuid import uuid4
import sqlite3

from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository
# Utilitários de infra para padronizar logs de movimentação
from services.ledger.service_ledger_infra import _ensure_mov_cols, _fmt_obs_saida

_EPS = 1e-9  # Tolerância numérica para comparações de ponto flutuante


@dataclass
class ResultadoParcela:
    """Snapshot resumido do efeito do pagamento em uma parcela de fatura.

    Attributes:
        parcela_id: ID da parcela afetada.
        aplicado_principal: Valor de principal aplicado nesta parcela (amortização).
        aplicado_juros: Valor de juros aplicado nesta parcela (somente 1ª aberta).
        aplicado_multa: Valor de multa aplicado nesta parcela (somente 1ª aberta).
        aplicado_desconto: Valor de desconto aplicado nesta parcela (somente 1ª aberta).
        saida_total: Impacto total de caixa/banco nesta parcela
            (**principal + juros + multa**; desconto **não** sai do caixa), calculado no Repository.
        status: Novo status da parcela após a aplicação (e.g., "PARCIAL", "QUITADO").
        restante_principal: Principal ainda em aberto após a aplicação nesta parcela.
    """
    parcela_id: int
    aplicado_principal: float
    aplicado_juros: float
    aplicado_multa: float
    aplicado_desconto: float
    saida_total: float
    status: str
    restante_principal: float


class ServiceLedgerFatura:
    """Serviço de pagamento de FATURA_CARTAO com rateio FIFO e encargos na 1ª parcela aberta."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.cap_repo = ContasAPagarMovRepository(db_path)

    # ------------------------------------------------------------------
    # API compatível com o helper legado (_pagar_fatura_por_obrigacao)
    # ------------------------------------------------------------------
    def pagar_fatura_cartao(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Wrapper compatível:
        - Aceita `conn` como 1º argumento posicional (opcional).
        - Aceita `valor_base` **ou** `valor` (alias para principal) ou `principal`.
        - Aceita `juros/multa/desconto`, `data_evento`, `usuario`.
        - Usa (quando fornecidos) `forma_pagamento` e `origem` (ou `banco_nome`/`origem_dinheiro`)
          para registrar uma linha em `movimentacoes_bancarias` quando o pagamento
          é feito diretamente por este serviço (sem passar por registrar_saida_*).
        - Retorna dict com `ok`, `sobra`, `obrigacao_id`, `ids`, `trans_uid`,
          `saida_total` (dinheiro que sai: principal + juros + multa), `resultados`,
          e opcional `mensagem`.
        """
        conn: Optional[sqlite3.Connection] = None
        if args and isinstance(args[0], sqlite3.Connection):
            conn = args[0]

        obrigacao_id = int(kwargs.get("obrigacao_id"))

        # Mapear principal (alias)
        if "valor_base" in kwargs:
            principal = float(kwargs.get("valor_base") or 0.0)
        elif "valor" in kwargs:
            principal = float(kwargs.get("valor") or 0.0)
        else:
            principal = float(kwargs.get("principal") or 0.0)

        juros = float(kwargs.get("juros") or 0.0)
        multa = float(kwargs.get("multa") or 0.0)
        desconto = float(kwargs.get("desconto") or 0.0)
        data_evento = kwargs.get("data_evento")
        usuario = kwargs.get("usuario") or "-"

        # Infos para possível log de movimentação direta
        forma_pagamento = (kwargs.get("forma_pagamento") or kwargs.get("forma") or "DINHEIRO").upper()
        if forma_pagamento == "DEBITO":
            forma_pagamento = "DÉBITO"
        origem = (
            kwargs.get("origem")
            or kwargs.get("banco_nome")
            or kwargs.get("origem_dinheiro")
            or ("Caixa" if forma_pagamento == "DINHEIRO" else "Banco 1")
        )

        ledger_id = kwargs.get("ledger_id")  # quando pagamento veio do fluxo de SAÍDA
        trans_uid_ext = kwargs.get("trans_uid")  # se vier de fora, preservamos

        core = self._pagar_core(
            obrigacao_id=obrigacao_id,
            principal=principal,
            juros=juros,
            multa=multa,
            desconto=desconto,
            data_evento=data_evento,
            usuario=usuario,
            conn=conn,
            trans_uid_override=trans_uid_ext,
        )

        # Se NÃO veio do fluxo de SAÍDA (sem ledger_id), registramos a movimentação aqui.
        # Evita duplicidade usando o mesmo trans_uid.
        mov_id: Optional[int] = None
        if ledger_id is None and core["saida_total"] > 0:
            with self._conn_ctx(conn) as c:
                if not self._mov_ja_existe(c, core["trans_uid"]):
                    _ensure_mov_cols(c.cursor())
                    obs = _fmt_obs_saida(
                        forma=forma_pagamento,
                        valor=float(core["saida_total"]),
                        categoria="FATURA_CARTAO",
                        subcategoria=None,
                        descricao=f"Pagamento fatura (obrigação {obrigacao_id})",
                        banco=(origem if forma_pagamento == "DÉBITO" else None),
                    )
                    cur = c.execute(
                        """
                        INSERT INTO movimentacoes_bancarias
                            (data, banco, tipo, valor, origem, observacao,
                             referencia_tabela, referencia_id, trans_uid, usuario, data_hora)
                        VALUES (?, ?, 'saida', ?, 'saidas', ?, 'fatura_cartao', ?, ?, ?, ?)
                        """,
                        (
                            (data_evento or datetime.now().strftime("%Y-%m-%d")),
                            origem,
                            float(core["saida_total"]),
                            obs,
                            int(obrigacao_id),
                            core["trans_uid"],
                            usuario,
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        ),
                    )
                    mov_id = int(cur.lastrowid)

        # Monta resposta padronizada (esperada pelo helper compat)
        resp = {
            "ok": True,
            "obrigacao_id": obrigacao_id,
            "sobra": float(core["sobra"]),
            "trans_uid": core["trans_uid"],
            "saida_total": float(core["saida_total"]),  # dinheiro que sai: principal + juros + multa
            "resultados": core["resultados"],
            "ids": {
                "ledger_id": ledger_id if ledger_id is not None else None,
                "cap_evento_id": None,
                "mov_id": mov_id,
            },
        }
        if core.get("mensagem"):
            resp["ok"] = False
            resp["mensagem"] = core["mensagem"]
        return resp

    # ------------------------------------------------------------------
    # API principal (mantida) — agora também retorna `sobra`
    # ------------------------------------------------------------------
    def pagar_fatura(
        self,
        *,
        obrigacao_id: int,
        principal: float,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        data_evento: Optional[str] = None,
        usuario: str = "-",
        conn: Optional[sqlite3.Connection] = None,
    ) -> Dict[str, Any]:
        """Aplica o pagamento de fatura de cartão conforme regras FIFO e de encargos."""
        core = self._pagar_core(
            obrigacao_id=obrigacao_id,
            principal=principal,
            juros=juros,
            multa=multa,
            desconto=desconto,
            data_evento=data_evento,
            usuario=usuario,
            conn=conn,
        )
        return {
            "trans_uid": core["trans_uid"],
            "saida_total": float(core["saida_total"]),  # dinheiro que sai: principal + juros + multa
            "resultados": core["resultados"],
            "sobra": float(core["sobra"]),
            **({"mensagem": core["mensagem"]} if core.get("mensagem") else {}),
        }

    # ------------------------------------------------------------------
    # Núcleo compartilhado
    # ------------------------------------------------------------------
    def _pagar_core(
        self,
        *,
        obrigacao_id: int,
        principal: float,
        juros: float,
        multa: float,
        desconto: float,
        data_evento: Optional[str],
        usuario: str,
        conn: Optional[sqlite3.Connection],
        trans_uid_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Núcleo de aplicação do pagamento com retorno padronizado para wrappers."""
        data_evt = data_evento or datetime.now().strftime("%Y-%m-%d")
        trans_uid = (
            trans_uid_override
            if trans_uid_override
            else f"FATURA-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
        )

        # Sanitização (não permitir negativos)
        restante_principal = max(0.0, float(principal or 0.0))
        juros = max(0.0, float(juros or 0.0))
        multa = max(0.0, float(multa or 0.0))
        desconto = max(0.0, float(desconto or 0.0))

        resultados: List[ResultadoParcela] = []
        saida_total_agregado = 0.0

        with self._conn_ctx(conn) as c:
            fifo = self.cap_repo.listar_parcelas_em_aberto_fifo(c, obrigacao_id=obrigacao_id)
            if not fifo:
                return {
                    "trans_uid": trans_uid,
                    "saida_total": 0.0,
                    "resultados": [],
                    "sobra": float(restante_principal),
                    "mensagem": "Nenhuma parcela em aberto para esta fatura.",
                }

            encargos_pend = {"juros": juros, "multa": multa, "desconto": desconto}
            primeira = True

            for p in fifo:
                if restante_principal <= _EPS and not primeira:
                    break

                parcela_id = int(p["parcela_id"])
                faltante_parcela = float(p["principal_faltante"] or 0.0)

                aplicar_principal = min(restante_principal, max(0.0, faltante_parcela))
                restante_principal = round(restante_principal - aplicar_principal, 2)

                if primeira:
                    aplicar_juros = encargos_pend["juros"]
                    aplicar_multa = encargos_pend["multa"]
                    aplicar_desconto = encargos_pend["desconto"]
                    encargos_pend = {"juros": 0.0, "multa": 0.0, "desconto": 0.0}
                    primeira = False
                else:
                    aplicar_juros = 0.0
                    aplicar_multa = 0.0
                    aplicar_desconto = 0.0

                snap = self.cap_repo.aplicar_pagamento_parcela(
                    c,
                    parcela_id=parcela_id,
                    valor_base=aplicar_principal,
                    juros=aplicar_juros,
                    multa=aplicar_multa,
                    desconto=aplicar_desconto,
                    data_evento=data_evt,
                    usuario=usuario,
                )

                saida_total_agregado = round(saida_total_agregado + float(snap["saida_total"]), 2)

                resultados.append(
                    ResultadoParcela(
                        parcela_id=parcela_id,
                        aplicado_principal=float(snap["principal_aplicado"]),
                        aplicado_juros=float(snap["juros_aplicado"]),
                        aplicado_multa=float(snap["multa_aplicada"]),
                        aplicado_desconto=float(snap["desconto_aplicado"]),
                        saida_total=float(snap["saida_total"]),
                        status=str(snap["status"]),
                        restante_principal=float(snap["restante"]),
                    )
                )

                if restante_principal <= _EPS and aplicar_principal <= _EPS:
                    break

        return {
            "trans_uid": trans_uid,
            "saida_total": float(saida_total_agregado),  # dinheiro que sai: principal + juros + multa
            "resultados": [r.__dict__ for r in resultados],
            "sobra": float(restante_principal),
        }

    # ------------------------------------------------------------------
    # Movimentação direta (quando não veio do fluxo de SAÍDA)
    # ------------------------------------------------------------------
    def _mov_ja_existe(self, conn: sqlite3.Connection, trans_uid: str) -> bool:
        cur = conn.execute("SELECT 1 FROM movimentacoes_bancarias WHERE trans_uid = ? LIMIT 1", (trans_uid,))
        return cur.fetchone() is not None

    # ------------------------------------------------------------------
    # Utilitário interno: contexto de conexão/commit
    # ------------------------------------------------------------------
    @contextmanager
    def _conn_ctx(self, conn: Optional[sqlite3.Connection]) -> Iterator[sqlite3.Connection]:
        """Gerencia a conexão SQLite (reusa a existente ou cria/commita/fecha)."""
        if conn is not None:
            yield conn
        else:
            c = sqlite3.connect(self.db_path)
            try:
                yield c
            finally:
                c.commit()
                c.close()


# --- Retrocompat (mantém nomes esperados por imports antigos) ---
FaturaCartaoMixin = ServiceLedgerFatura
FaturaLedgerMixin = ServiceLedgerFatura
_FaturaLedgerMixin = ServiceLedgerFatura

__all__ = [
    "ServiceLedgerFatura",
    "FaturaCartaoMixin",
    "FaturaLedgerMixin",
    "_FaturaLedgerMixin",
    "ResultadoParcela",
]
