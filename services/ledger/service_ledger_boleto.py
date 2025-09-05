# services/ledger/service_ledger_boleto.py
"""
Service: BOLETO (pagamento de parcelas)

Regras principais:
- FIFO por vencimento das parcelas em aberto da obrigação.
- Rateio do pagamento:
    • principal → amortiza as parcelas (CASCADE entre parcelas).
    • juros     → aplica apenas na PRIMEIRA parcela ainda aberta (não cascateia).
    • multa     → aplica apenas na PRIMEIRA parcela ainda aberta (não cascateia).
    • desconto  → aplica apenas na PRIMEIRA parcela ainda aberta (não cascateia).
- Saída de caixa/banco: **principal + juros + multa** (desconto NÃO sai do caixa), agregado.
- trans_uid: gerado por operação (idempotência).
- Este serviço NÃO cria novos eventos no CAP; apenas aplica nos LANCAMENTOS já existentes.
- Movimentação bancária:
    • se o pagamento vier do fluxo de SAÍDA (registrar_saida_*), ela é registrada lá;
    • se o pagamento for direto por aqui, registramos 1 linha em `movimentacoes_bancarias`
      com checagem de idempotência via `trans_uid`.

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
from services.ledger.service_ledger_infra import _fmt_obs_saida, log_mov_bancaria

_EPS = 1e-9  # Tolerância numérica para comparações de ponto flutuante


@dataclass
class ResultadoParcela:
    """Snapshot resumido do efeito do pagamento em uma parcela."""
    parcela_id: int
    aplicado_principal: float
    aplicado_juros: float
    aplicado_multa: float
    aplicado_desconto: float
    saida_total: float
    status: str
    restante_principal: float


class ServiceLedgerBoleto:
    """Serviço de pagamento de BOLETO com rateio FIFO e encargos na 1ª parcela aberta."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.cap_repo = ContasAPagarMovRepository(db_path)

    # ------------------------------------------------------------------
    # APIs públicas
    # ------------------------------------------------------------------
    def pagar_parcela_boleto(
        self,
        *,
        obrigacao_id: int,
        parcela_id: Optional[int] = None,  # ignorado (pagamento é FIFO)
        valor_base: float,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        usuario: str | None = None,
        data_evento: Optional[str] = None,
        conn: Optional[sqlite3.Connection] = None,
    ) -> Dict[str, Any]:
        """
        Aplica pagamento nas parcelas da obrigação (FIFO).
        NÃO cria movimentação bancária aqui — indicado quando a Saída foi registrada em outro fluxo.
        """
        core = self._pagar_core(
            obrigacao_id=obrigacao_id,
            principal=float(valor_base or 0.0),
            juros=float(juros or 0.0),
            multa=float(multa or 0.0),
            desconto=float(desconto or 0.0),
            data_evento=data_evento,
            usuario=usuario or "-",
            conn=conn,
        )
        return {
            "trans_uid": core["trans_uid"],
            "saida_total": float(core["saida_total"]),
            "resultados": core["resultados"],
            "sobra": float(core["sobra"]),
            **({"mensagem": core["mensagem"]} if core.get("mensagem") else {}),
        }

    def pagar_boleto(
        self,
        *,
        obrigacao_id: int,
        principal: float,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        data_evento: Optional[str] = None,
        usuario: str = "-",
        forma_pagamento: Optional[str] = None,
        origem: Optional[str] = None,
        ledger_id: Optional[int] = None,
        conn: Optional[sqlite3.Connection] = None,
    ) -> Dict[str, Any]:
        """
        Paga boleto diretamente por aqui.
        Se `ledger_id` vier None, registra 1 movimento bancário com idempotência por `trans_uid`.
        """
        forma = (forma_pagamento or "DINHEIRO").upper()
        if forma == "DEBITO":
            forma = "DÉBITO"
        origem_eff = origem or ("Caixa" if forma == "DINHEIRO" else "Banco 1")

        core = self._pagar_core(
            obrigacao_id=obrigacao_id,
            principal=float(principal or 0.0),
            juros=float(juros or 0.0),
            multa=float(multa or 0.0),
            desconto=float(desconto or 0.0),
            data_evento=data_evento,
            usuario=usuario or "-",
            conn=conn,
        )

        mov_id: Optional[int] = None
        if ledger_id is None and core["saida_total"] > 0:
            with self._conn_ctx(conn) as c:
                # evite duplicar a movimentação (idempotência pelo trans_uid)
                if not self._mov_ja_existe(c, core["trans_uid"]):
                    obs = _fmt_obs_saida(
                        forma=forma,
                        valor=float(core["saida_total"]),
                        categoria="BOLETO",
                        subcategoria=None,
                        descricao=f"Pagamento boleto (obrigação {obrigacao_id})",
                        banco=(origem_eff if forma == "DÉBITO" else None),
                    )
                    mov_id = log_mov_bancaria(
                        c,
                        data=(data_evento or datetime.now().strftime("%Y-%m-%d")),
                        banco=str(origem_eff or ""),
                        tipo="saida",
                        valor=float(core["saida_total"]),
                        origem="saidas",
                        observacao=obs,
                        usuario=usuario,
                        referencia_id=int(obrigacao_id),
                        referencia_tabela="contas_a_pagar_mov",
                        trans_uid=str(core["trans_uid"]),
                        data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )

        return {
            "ok": True,
            "obrigacao_id": obrigacao_id,
            "sobra": float(core["sobra"]),
            "trans_uid": core["trans_uid"],
            "saida_total": float(core["saida_total"]),
            "resultados": core["resultados"],
            "ids": {
                "ledger_id": ledger_id,
                "cap_evento_id": None,
                "mov_id": mov_id if (ledger_id is None and core["saida_total"] > 0) else None,
            },
            **({"mensagem": core["mensagem"]} if core.get("mensagem") else {}),
        }

    def registrar_saida_boleto(
        self,
        *,
        valor: float,
        forma: Optional[str] = None,
        origem: Optional[str] = None,
        banco: Optional[str] = None,
        descricao: Optional[str] = None,
        usuario: Optional[str] = None,
        trans_uid: Optional[str] = None,
        obrigacao_id: Optional[int] = None,
        parcela_id: Optional[int] = None,  # ignorado (FIFO)
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        data_evento: Optional[str] = None,
        **_ignored: Any,
    ) -> Dict[str, Any]:
        """
        Wrapper usado pelo fluxo de Saídas:
        - Com `obrigacao_id`: aplica pagamento FIFO no CAP (NÃO cria MB aqui).
        - Sem `obrigacao_id`: este serviço não lida com saída avulsa; retorna ok=False.
        """
        if obrigacao_id is None:
            return {
                "ok": False,
                "mensagem": "registrar_saida_boleto sem obrigacao_id não é suportado neste serviço.",
            }

        core = self._pagar_core(
            obrigacao_id=int(obrigacao_id),
            principal=float(valor or 0.0),
            juros=float(juros or 0.0),
            multa=float(multa or 0.0),
            desconto=float(desconto or 0.0),
            data_evento=data_evento,
            usuario=usuario or "-",
            conn=None,
            trans_uid_override=trans_uid,  # preserva idempotência entre camadas
        )
        return {
            "ok": True,
            "obrigacao_id": int(obrigacao_id),
            "sobra": float(core["sobra"]),
            "trans_uid": core["trans_uid"],
            "saida_total": float(core["saida_total"]),
            "resultados": core["resultados"],
            "ids": {"ledger_id": None, "cap_evento_id": None, "mov_id": None},
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
        """Aplica o pagamento seguindo FIFO e regras de encargos (desconto não sai do caixa)."""
        data_evt = data_evento or datetime.now().strftime("%Y-%m-%d")
        trans_uid = (
            trans_uid_override
            if trans_uid_override
            else f"BOLETO-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
        )

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
                    "mensagem": "Nenhuma parcela em aberto para este boleto.",
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
            "saida_total": float(saida_total_agregado),
            "resultados": [r.__dict__ for r in resultados],
            "sobra": float(restante_principal),
        }

    # ------------------------------------------------------------------
    # Utilitários internos
    # ------------------------------------------------------------------
    def _mov_ja_existe(self, conn: sqlite3.Connection, trans_uid: str) -> bool:
        cur = conn.execute("SELECT 1 FROM movimentacoes_bancarias WHERE trans_uid = ? LIMIT 1", (trans_uid,))
        return cur.fetchone() is not None

    @contextmanager
    def _conn_ctx(self, conn: Optional[sqlite3.Connection]) -> Iterator[sqlite3.Connection]:
        if conn is not None:
            yield conn
        else:
            c = sqlite3.connect(self.db_path)
            try:
                yield c
            finally:
                c.commit()
                c.close()


# --- Retrocompat (mantém nomes antigos esperados por imports legados) ---
BoletoMixin = ServiceLedgerBoleto
_BoletoLedgerMixin = BoletoMixin
__all__ = ["BoletoMixin", "_BoletoLedgerMixin", "ServiceLedgerBoleto", "ResultadoParcela"]
