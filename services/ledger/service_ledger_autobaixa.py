# services/ledger/service_ledger_autobaixa.py
"""
Auto-baixa (conciliação simples) para CAP

Objetivo
--------
Fornecer utilitários para vincular um movimento bancário já existente
(`movimentacoes_bancarias`) a uma obrigação no CAP (BOLETO, EMPRESTIMO
ou FATURA_CARTAO) aplicando o pagamento pelo serviço correto.

Princípios
----------
- Não cria linhas novas no CAP além da aplicação do pagamento.
- Não duplica movimentação: quando usar o serviço de fatura, passamos
  `ledger_id` para o serviço não registrar outra `movimentacoes_bancarias`.
- Status/faltante seguem o padrão novo (baseados no PRINCIPAL amortizado).
- Desconto **não** sai do caixa (mantido pelo repository/serviços).

APIs principais
---------------
- auto_baixar_movimento(...): concilia por `mov_id` ou `trans_uid`.
- auto_baixar_por_valor_data(...): busca o movimento por data/valor e concilia.
- auto_baixar_por_trans_uid(...): atalho por UID.

Retornos
--------
Sempre retorna um dict com chaves:
    ok (bool), mensagem (str opcional), mov_id (int|None), obrigacao_id (int),
    tipo_obrigacao (str), saida_total (float), resultados (list[dict]),
    sobra (float, quando aplicável), trans_uid (str)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from datetime import datetime
import sqlite3

from shared.db import get_conn
from services.ledger.service_ledger_boleto import ServiceLedgerBoleto
from services.ledger.service_ledger_fatura import ServiceLedgerFatura
from services.ledger.service_ledger_infra import vincular_mov_a_parcela_boleto

__all__ = ["_AutoBaixaLedgerMixin"]


@dataclass
class _MovRow:
    id: int
    data: str
    banco: str
    tipo: str
    valor: float
    origem: str
    observacao: str
    trans_uid: Optional[str]


class _AutoBaixaLedgerMixin:
    """
    Mixin de auto-baixa. Assume que o objeto "pai" expõe `self.db_path`.
    """

    # ------------------------- utils internos -------------------------
    def _fetch_mov_by_id(self, conn: sqlite3.Connection, mov_id: int) -> Optional[_MovRow]:
        cur = conn.execute(
            """
            SELECT id, data, banco, tipo, COALESCE(valor,0) AS valor, origem,
                   COALESCE(observacao,'') AS observacao, trans_uid
              FROM movimentacoes_bancarias
             WHERE id = ?
             LIMIT 1
            """,
            (int(mov_id),),
        )
        r = cur.fetchone()
        if not r:
            return None
        return _MovRow(
            id=int(r["id"]),
            data=str(r["data"]),
            banco=str(r["banco"] or ""),
            tipo=str(r["tipo"] or ""),
            valor=float(r["valor"] or 0.0),
            origem=str(r["origem"] or ""),
            observacao=str(r["observacao"] or ""),
            trans_uid=(r["trans_uid"] if r["trans_uid"] is not None else None),
        )

    def _fetch_mov_by_trans_uid(self, conn: sqlite3.Connection, trans_uid: str) -> Optional[_MovRow]:
        cur = conn.execute(
            """
            SELECT id, data, banco, tipo, COALESCE(valor,0) AS valor, origem,
                   COALESCE(observacao,'') AS observacao, trans_uid
              FROM movimentacoes_bancarias
             WHERE trans_uid = ?
             LIMIT 1
            """,
            (str(trans_uid),),
        )
        r = cur.fetchone()
        if not r:
            return None
        return _MovRow(
            id=int(r["id"]),
            data=str(r["data"]),
            banco=str(r["banco"] or ""),
            tipo=str(r["tipo"] or ""),
            valor=float(r["valor"] or 0.0),
            origem=str(r["origem"] or ""),
            observacao=str(r["observacao"] or ""),
            trans_uid=(r["trans_uid"] if r["trans_uid"] is not None else None),
        )

    def _fetch_first_mov_by_valor_data(
        self, conn: sqlite3.Connection, *, data: str, valor: float
    ) -> Optional[_MovRow]:
        cur = conn.execute(
            """
            SELECT id, data, banco, tipo, COALESCE(valor,0) AS valor, origem,
                   COALESCE(observacao,'') AS observacao, trans_uid
              FROM movimentacoes_bancarias
             WHERE DATE(data) = DATE(?)
               AND ABS(COALESCE(valor,0) - ?) < 0.005
               AND LOWER(tipo) = 'saida'
             ORDER BY id DESC
             LIMIT 1
            """,
            (str(data), float(valor)),
        )
        r = cur.fetchone()
        if not r:
            return None
        return _MovRow(
            id=int(r["id"]),
            data=str(r["data"]),
            banco=str(r["banco"] or ""),
            tipo=str(r["tipo"] or ""),
            valor=float(r["valor"] or 0.0),
            origem=str(r["origem"] or ""),
            observacao=str(r["observacao"] or ""),
            trans_uid=(r["trans_uid"] if r["trans_uid"] is not None else None),
        )

    def _append_obs(self, conn: sqlite3.Connection, mov_id: int, extra: str) -> None:
        conn.execute(
            """
            UPDATE movimentacoes_bancarias
               SET observacao = TRIM(COALESCE(observacao,'') || ' ' || ?)
             WHERE id = ?
            """,
            (str(extra), int(mov_id)),
        )

    # --------------------- APIs públicas de conciliação ---------------------

    def auto_baixar_por_trans_uid(
        self,
        *,
        trans_uid: str,
        tipo_obrigacao: str,
        obrigacao_id: int,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        usuario: str = "-",
        data_evento: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Atalho: encontra o movimento pelo `trans_uid` e concilia com a obrigação."""
        with get_conn(self.db_path) as conn:  # type: ignore[attr-defined]
            mov = self._fetch_mov_by_trans_uid(conn, trans_uid)
            if not mov:
                return {"ok": False, "mensagem": f"Movimentação com trans_uid '{trans_uid}' não encontrada."}
        return self.auto_baixar_movimento(
            mov_id=mov.id,
            tipo_obrigacao=tipo_obrigacao,
            obrigacao_id=obrigacao_id,
            juros=juros,
            multa=multa,
            desconto=desconto,
            usuario=usuario,
            data_evento=(data_evento or mov.data),
        )

    def auto_baixar_por_valor_data(
        self,
        *,
        data: str,
        valor: float,
        tipo_obrigacao: str,
        obrigacao_id: int,
        usuario: str = "-",
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
    ) -> Dict[str, Any]:
        """Busca o primeiro movimento "saída" com mesma data/valor e concilia com a obrigação."""
        with get_conn(self.db_path) as conn:  # type: ignore[attr-defined]
            mov = self._fetch_first_mov_by_valor_data(conn, data=data, valor=float(valor))
            if not mov:
                return {"ok": False, "mensagem": "Nenhuma movimentação compatível encontrada (data/valor)."}
            mid = int(mov.id)
        return self.auto_baixar_movimento(
            mov_id=mid,
            tipo_obrigacao=tipo_obrigacao,
            obrigacao_id=obrigacao_id,
            usuario=usuario,
            juros=juros,
            multa=multa,
            desconto=desconto,
            data_evento=data,
        )

    def auto_baixar_movimento(
        self,
        *,
        mov_id: int,
        tipo_obrigacao: str,
        obrigacao_id: int,
        usuario: str = "-",
        data_evento: Optional[str] = None,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Concilia (auto-baixa) **um** movimento `saida` com a obrigação informada.

        Regras:
        - Usa o valor do movimento como "caixa" disponível.
        - Aplica o pagamento pelo serviço correspondente:
            • FATURA_CARTAO -> ServiceLedgerFatura.pagar_fatura_cartao (sem duplicar mov.)
            • BOLETO/EMPRESTIMO -> ServiceLedgerBoleto.pagar_boleto
        - Vincula o `mov_id` à 1ª parcela afetada quando houver (boletos/emprestimos).
        """
        tipo = (tipo_obrigacao or "").strip().upper()
        if tipo not in {"FATURA_CARTAO", "BOLETO", "EMPRESTIMO"}:
            return {"ok": False, "mensagem": f"tipo_obrigacao inválido: {tipo_obrigacao!r}"}

        with get_conn(self.db_path) as conn:  # type: ignore[attr-defined]
            conn.row_factory = sqlite3.Row
            mov = self._fetch_mov_by_id(conn, int(mov_id))
            if not mov:
                return {"ok": False, "mensagem": f"Movimentação id={mov_id} não encontrada."}
            if str(mov.tipo).lower() != "saida":
                return {"ok": False, "mensagem": "A movimentação informada não é do tipo 'saida'."}

            # Caixa disponível (dinheiro que saiu no movimento)
            caixa_total = float(mov.valor or 0.0)
            if caixa_total <= 0:
                return {"ok": False, "mensagem": "Valor do movimento inválido para auto-baixa."}

            data_evt = data_evento or mov.data or datetime.now().strftime("%Y-%m-%d")
            usuario = (usuario or "-").strip() or "-"

            if tipo == "FATURA_CARTAO":
                svc = ServiceLedgerFatura(self.db_path)  # type: ignore[attr-defined]
                res = svc.pagar_fatura_cartao(
                    conn,
                    obrigacao_id=int(obrigacao_id),
                    valor_base=float(caixa_total),
                    juros=float(juros or 0.0),
                    multa=float(multa or 0.0),
                    desconto=float(desconto or 0.0),
                    data_evento=str(data_evt),
                    forma_pagamento="DÉBITO" if mov.banco and mov.banco.strip() else "DINHEIRO",
                    origem=(mov.banco or "Caixa"),
                    usuario=usuario,
                    ledger_id=int(mov.id),  # evita duplicar movimentação
                    trans_uid=(mov.trans_uid or None),
                )
                # Não há vínculo de parcela específico (fatura agrega várias)
                saida_total = float(res.get("saida_total", 0.0))
                sobra = float(res.get("sobra", 0.0))
                trans_uid = res.get("trans_uid", (mov.trans_uid or ""))
                resultados = res.get("resultados", [])

            else:
                # BOLETO / EMPRESTIMO usam o mesmo core FIFO do service de boletos
                svc = ServiceLedgerBoleto(self.db_path)  # type: ignore[attr-defined]
                res = svc.pagar_boleto(
                    obrigacao_id=int(obrigacao_id),
                    principal=float(caixa_total),
                    juros=float(juros or 0.0),
                    multa=float(multa or 0.0),
                    desconto=float(desconto or 0.0),
                    data_evento=str(data_evt),
                    usuario=usuario,
                    conn=conn,
                )
                saida_total = float(res.get("saida_total", 0.0))
                sobra = float(caixa_total) - sum(
                    float(r.get("aplicado_principal", 0.0)) for r in res.get("resultados", [])
                )
                sobra = max(0.0, round(sobra, 2))
                trans_uid = res.get("trans_uid", (mov.trans_uid or ""))

                # Vincula o movimento à 1ª parcela afetada (se houver)
                try:
                    resultados = res.get("resultados", [])
                    if resultados:
                        parcela_id = int(resultados[0].get("parcela_id"))
                        vincular_mov_a_parcela_boleto(self.db_path, int(mov.id), parcela_id)
                except Exception:
                    pass

            # Marca na observação que a conciliação foi feita
            try:
                self._append_obs(conn, int(mov.id), "[autobaixa OK]")
            except Exception:
                pass

        return {
            "ok": True,
            "tipo_obrigacao": tipo,
            "obrigacao_id": int(obrigacao_id),
            "mov_id": int(mov_id),
            "saida_total": float(saida_total),
            "sobra": float(sobra),
            "trans_uid": trans_uid,
            "resultados": resultados,
        }
