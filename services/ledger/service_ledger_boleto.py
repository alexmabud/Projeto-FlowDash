# services/ledger/service_ledger_boleto.py
"""
Service: BOLETO (pagamento de parcelas)

Regras principais deste serviço:
- Ordem de aplicação: FIFO por vencimento das parcelas em aberto na obrigação.
- Rateio do pagamento:
    • principal → amortiza as parcelas (CASCADE entre parcelas).
    • juros     → aplica apenas na PRIMEIRA parcela ainda aberta (não cascateia).
    • multa     → aplica apenas na PRIMEIRA parcela ainda aberta (não cascateia).
    • desconto  → aplica apenas na PRIMEIRA parcela ainda aberta (não cascateia).
- Saída de caixa/banco: o Repository calcula o caixa gasto de cada aplicação
  como **(principal + juros + multa)** — **desconto não sai do caixa** — e consolida a movimentação.
- trans_uid: gerado por operação e retornado ao chamador (não é persistido aqui).
- Este serviço NÃO cria eventos novos no CAP; ele aplica/acumula nos lançamentos
  já programados (parcelas).

Dependências:
- repository.contas_a_pagar_mov_repository.ContasAPagarMovRepository
  • listar_parcelas_em_aberto_fifo(conn, obrigacao_id)
  • aplicar_pagamento_parcela(conn, parcela_id, valor_base, juros, multa, desconto, data_evento, usuario)
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
    """Snapshot resumido do efeito do pagamento em uma parcela.

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


class ServiceLedgerBoleto:
    """Serviço de pagamento de BOLETOS (parcelas) com rateio FIFO e encargos na 1ª aberta."""

    def __init__(self, db_path: str):
        """Inicializa o serviço.

        Args:
            db_path: Caminho do arquivo SQLite.
        """
        self.db_path = db_path
        self.cap_repo = ContasAPagarMovRepository(db_path)

    # ------------------------------------------------------------------
    # API moderna
    # ------------------------------------------------------------------
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
        conn: Optional[sqlite3.Connection] = None,
    ) -> Dict[str, Any]:
        """Aplica o pagamento de BOLETO seguindo as regras FIFO e de encargos.

        Regras:
            - O `principal` é aplicado em cascata sobre as parcelas em aberto (FIFO).
            - `juros`, `multa` e `desconto` são aplicados apenas na PRIMEIRA parcela
              ainda em aberto no momento da operação (não cascateiam).

        Args:
            obrigacao_id: Identificador do grupo de parcelas (obrigação).
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
            - O cálculo da saída (**principal + juros + multa**; desconto não entra) é feito no Repository.
            - Este serviço não cria linhas em CAP; apenas aplica/acumula na programação existente.
        """
        data_evt = data_evento or datetime.now().strftime("%Y-%m-%d")
        trans_uid = f"BOLETO-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"

        # Sanitização (não permitir negativos)
        restante_principal = max(0.0, float(principal or 0.0))
        juros = max(0.0, float(juros or 0.0))
        multa = max(0.0, float(multa or 0.0))
        desconto = max(0.0, float(desconto or 0.0))

        resultados: List[ResultadoParcela] = []
        saida_total_agregado = 0.0

        with self._conn_ctx(conn) as c:
            # 1) Obter parcelas em aberto (FIFO)
            fifo = self.cap_repo.listar_parcelas_em_aberto_fifo(c, obrigacao_id=obrigacao_id)
            if not fifo:
                return {
                    "trans_uid": trans_uid,
                    "saida_total": 0.0,
                    "resultados": [],
                    "mensagem": "Nenhuma parcela em aberto para esta obrigação.",
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
    # Retrocompat: aliases/shims esperados por código legado
    # ------------------------------------------------------------------
    def pagar_parcela_boleto(
        self,
        *,
        obrigacao_id: int,
        parcela_id: Optional[int] = None,  # Aceito por compat; ignorado (pagamento é FIFO)
        valor_base: float,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        usuario: str = "-",
        data_evento: Optional[str] = None,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        """Alias de compatibilidade que delega para `pagar_boleto`.

        Observações:
            - `parcela_id` é aceito para manter compatibilidade, porém o pagamento
              segue FIFO por design.
            - Parâmetros extras (`forma`, `origem`, `banco`, `trans_uid`, etc.) são
              ignorados aqui; o débito efetivo em caixa/banco é resolvido no Repository.

        Args:
            obrigacao_id: Identificador do grupo de parcelas.
            parcela_id: Ignorado; mantido para compatibilidade.
            valor_base: Valor de principal a aplicar.
            juros: Juros a aplicar (na 1ª parcela aberta).
            multa: Multa a aplicar (na 1ª parcela aberta).
            desconto: Desconto a aplicar (na 1ª parcela aberta).
            usuario: Operador.
            data_evento: Data do evento 'YYYY-MM-DD'.
            **_kwargs: Parâmetros legados ignorados.

        Returns:
            Mesmo dicionário retornado por `pagar_boleto`.
        """
        return self.pagar_boleto(
            obrigacao_id=obrigacao_id,
            principal=valor_base,
            juros=juros,
            multa=multa,
            desconto=desconto,
            usuario=usuario,
            data_evento=data_evento,
        )

    def registrar_saida_boleto(
        self,
        *,
        valor: float,
        forma: Optional[str] = None,
        origem: Optional[str] = None,
        banco: Optional[str] = None,
        descricao: Optional[str] = None,
        usuario: str = "-",
        trans_uid: Optional[str] = None,
        obrigacao_id: Optional[int] = None,
        parcela_id: Optional[int] = None,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        data_evento: Optional[str] = None,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        """Shim de compatibilidade para cenários legados que chamavam 'registrar_saida_boleto'.

        Comportamento:
            - Se `obrigacao_id` for informado, redireciona para `pagar_parcela_boleto`
              (fluxo moderno, FIFO).
            - Se NÃO houver `obrigacao_id`, a operação é rejeitada explicitamente,
              pois este serviço não registra saídas avulsas de boletos sem vínculo de CAP.

        Args:
            valor: Valor a aplicar como principal (amortização).
            forma: (Ignorado neste serviço) Forma de pagamento.
            origem: (Ignorado neste serviço) Origem do recurso (Caixa/Caixa 2/Banco).
            banco: (Ignorado neste serviço) Nome do banco, quando aplicável.
            descricao: (Ignorado neste serviço) Descrição legada.
            usuario: Operador.
            trans_uid: (Ignorado) ID legada da transação.
            obrigacao_id: Obrigatório para que o pagamento seja aplicado via FIFO.
            parcela_id: Aceito para compat, mas ignorado (pagamento é FIFO).
            juros: Juros a aplicar na 1ª parcela aberta.
            multa: Multa a aplicar na 1ª parcela aberta.
            desconto: Desconto a aplicar na 1ª parcela aberta.
            data_evento: Data do evento 'YYYY-MM-DD'.
            **_kwargs: Parâmetros legados ignorados.

        Returns:
            Mesmo dicionário retornado por `pagar_parcela_boleto`.

        Raises:
            AttributeError: Se `obrigacao_id` não for informado.
        """
        if obrigacao_id is not None:
            return self.pagar_parcela_boleto(
                obrigacao_id=obrigacao_id,
                parcela_id=parcela_id,
                valor_base=valor,
                juros=juros,
                multa=multa,
                desconto=desconto,
                usuario=usuario,
                data_evento=data_evento,
                forma=forma,
                origem=origem,
                banco=banco,
                descricao=descricao,
                trans_uid=trans_uid,
            )

        raise AttributeError(
            "registrar_saida_boleto: uso sem 'obrigacao_id' não é suportado neste serviço. "
            "Registre saídas avulsas pelo registrador genérico de eventos/saídas."
        )

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


# --- Retrocompat (mantém nomes antigos esperados por imports legados) ---
BoletoMixin = ServiceLedgerBoleto
_BoletoLedgerMixin = BoletoMixin
__all__ = ["BoletoMixin", "_BoletoLedgerMixin", "ServiceLedgerBoleto", "ResultadoParcela"]
