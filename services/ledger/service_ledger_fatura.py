# services/ledger/service_ledger_fatura.py
"""
Service: FATURA_CARTAO (pagamento de parcelas de fatura de cartão)

Regras principais:
- Operamos sobre um `obrigacao_id` (a fatura do mês).
- Ordem de aplicação: FIFO por vencimento das parcelas em aberto da obrigação.
- Rateio do pagamento:
    • principal → vai para `valor_pago_acumulado` das parcelas (CASCADE entre parcelas).
    • juros     → vai para `juros_pago` apenas na PRIMEIRA parcela ainda aberta (não cascateia).
    • multa     → vai para `multa_paga` apenas na PRIMEIRA parcela ainda aberta (não cascateia).
    • desconto  → vai para `desconto_aplicado` apenas na PRIMEIRA parcela ainda aberta (não cascateia).
- Saída de caixa/banco: calculada pelo Repository como
  (principal + juros + multa − desconto) para cada aplicação.
- trans_uid: gerado por operação e retornado ao chamador.
- Este serviço NÃO cria eventos novos no CAP; apenas aplica/acumula nos lançamentos
  já programados (parcelas). A criação de Saída/Mov. bancária pode ser feita por
  outro serviço financeiro reutilizando o `saida_total` agregado.

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
            (principal + juros + multa − desconto), calculado no Repository.
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
        """Inicializa o serviço.

        Args:
            db_path: Caminho do arquivo SQLite.
        """
        self.db_path = db_path
        self.cap_repo = ContasAPagarMovRepository(db_path)

    # ------------------------------------------------------------------
    # API principal
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
        """Aplica o pagamento de fatura de cartão conforme regras FIFO e de encargos.

        Regras:
            - O `principal` é aplicado em cascata sobre as parcelas em aberto (FIFO).
            - `juros`, `multa` e `desconto` são aplicados apenas na PRIMEIRA parcela
              ainda em aberto no momento da operação (não cascateiam).

        Args:
            obrigacao_id: Identificador do grupo (fatura) a ser pago.
            principal: Valor destinado à amortização do principal (>= 0).
            juros: Encargo de juros a aplicar na 1ª parcela aberta (>= 0).
            multa: Encargo de multa a aplicar na 1ª parcela aberta (>= 0).
            desconto: Desconto a aplicar na 1ª parcela aberta (>= 0).
            data_evento: Data do evento no formato 'YYYY-MM-DD'. Padrão: data de hoje.
            usuario: Identificação do operador.
            conn: Conexão SQLite opcional. Se omitida, o serviço gerencia a transação.

        Returns:
            Dict com:
                - trans_uid (str): identificador único desta operação.
                - saida_total (float): soma do impacto de caixa/banco em todas as parcelas.
                - resultados (list[dict]): lista de snapshots por parcela aplicada.
                - (opcional) mensagem (str): quando não há parcelas em aberto.

        Notes:
            - O cálculo da saída (principal + juros + multa − desconto) é feito no Repository.
            - Este serviço não cria linhas em CAP; apenas aplica/acumula na programação existente.
        """
        data_evt = data_evento or datetime.now().strftime("%Y-%m-%d")
        trans_uid = f"FATURA-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"

        # Sanitização (não permitir negativos)
        restante_principal = max(0.0, float(principal or 0.0))
        juros = max(0.0, float(juros or 0.0))
        multa = max(0.0, float(multa or 0.0))
        desconto = max(0.0, float(desconto or 0.0))

        resultados: List[ResultadoParcela] = []
        saida_total_agregado = 0.0

        with self._conn_ctx(conn) as c:
            # 1) Obter parcelas em aberto (FIFO) desta fatura (obrigacao_id)
            fifo = self.cap_repo.listar_parcelas_em_aberto_fifo(c, obrigacao_id=obrigacao_id)
            if not fifo:
                return {
                    "trans_uid": trans_uid,
                    "saida_total": 0.0,
                    "resultados": [],
                    "mensagem": "Nenhuma parcela em aberto para esta fatura.",
                }

            # 2) Encargos são aplicados apenas na 1ª parcela aberta
            encargos_pend = {"juros": juros, "multa": multa, "desconto": desconto}
            primeira = True

            for p in fifo:
                # Se já não há principal a aplicar e já passamos da 1ª parcela (encargos aplicados), encerramos
                if restante_principal <= _EPS and not primeira:
                    break

                parcela_id = int(p["parcela_id"])
                faltante_parcela = float(p["principal_faltante"] or 0.0)

                aplicar_principal = min(restante_principal, max(0.0, faltante_parcela))
                # Atualiza o restante do principal (2 casas para manter consistência monetária)
                restante_principal = round(restante_principal - aplicar_principal, 2)

                if primeira:
                    aplicar_juros = encargos_pend["juros"]
                    aplicar_multa = encargos_pend["multa"]
                    aplicar_desconto = encargos_pend["desconto"]
                    # Zera pendências de encargos para as próximas parcelas
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

                # Se não há mais principal a aplicar e nada foi aplicado nesta iteração, encerramos
                if restante_principal <= _EPS and aplicar_principal <= _EPS:
                    break

        return {
            "trans_uid": trans_uid,
            "saida_total": float(saida_total_agregado),
            "resultados": [r.__dict__ for r in resultados],
        }

    # ------------------------------------------------------------------
    # Utilitário interno: contexto de conexão/commit
    # ------------------------------------------------------------------
    @contextmanager
    def _conn_ctx(self, conn: Optional[sqlite3.Connection]) -> Iterator[sqlite3.Connection]:
        """Gerencia a conexão SQLite.

        Se `conn` for fornecida, a função apenas a reutiliza (sem fechar/commit).
        Caso contrário, abre uma conexão nova, faz commit e fecha ao final.

        Args:
            conn: Conexão SQLite existente (opcional).

        Yields:
            Conexão SQLite utilizável dentro do bloco `with`.
        """
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
