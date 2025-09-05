# services/ledger/service_ledger_emprestimo.py
"""
Service: EMPRESTIMO/FINANCIAMENTO

Regras principais:
- Ordem de aplicação: FIFO por vencimento das parcelas em aberto.
- Rateio do pagamento:
    • principal → vai para `valor_pago_acumulado` das parcelas (CASCADE entre parcelas).
    • juros     → vai para `juros_pago` apenas na PRIMEIRA parcela ainda aberta (não cascateia).
    • multa     → vai para `multa_paga` apenas na PRIMEIRA parcela ainda aberta (não cascateia).
    • desconto  → vai para `desconto_aplicado` apenas na PRIMEIRA parcela ainda aberta (não cascateia).
- Saída de caixa/banco: **principal + juros + multa** (desconto **não** sai do caixa), agregado pelo Repository.
- trans_uid: gerado por operação e retornado ao chamador (aceita override para idempotência entre camadas).
- Este serviço NÃO cria eventos novos no CAP durante o pagamento; apenas aplica/acumula
  nos lançamentos já programados (parcelas). A movimentação bancária:
    • é registrada pelo fluxo de SAÍDA quando o pagamento vem de lá (preferível);
    • ou pode ser registrada aqui quando chamado diretamente (sem `ledger_id`), usando log central idempotente.

Programação de empréstimo:
- Cria N lançamentos no CAP (um por parcela), com `tipo_obrigacao='EMPRESTIMO'` e status inicial 'EM ABERTO'.

Dependências (Repository):
- listar_parcelas_em_aberto_fifo(conn, obrigacao_id)
- aplicar_pagamento_parcela(conn, parcela_id, valor_base, juros, multa, desconto, data_evento, usuario)
- registrar_lancamento(conn, obrigacao_id, tipo_obrigacao, valor_total, data_evento, vencimento, descricao, credor, competencia, parcela_num, parcelas_total, usuario, emprestimo_id)
- proximo_obrigacao_id(conn)  (opcional; com fallback de MAX(obrigacao_id))
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional
from uuid import uuid4
import sqlite3

import pandas as pd  # usado apenas para DateOffset no agendamento

from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository
from services.ledger.service_ledger_infra import _fmt_obs_saida, log_mov_bancaria

_EPS = 1e-9  # Tolerância numérica para comparações de ponto flutuante


@dataclass
class ResultadoParcela:
    """Snapshot resumido do efeito do pagamento em uma parcela de empréstimo.

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


class ServiceLedgerEmprestimo:
    """Serviço de pagamento e programação de EMPRESTIMO/FINANCIAMENTO."""

    def __init__(self, db_path: str) -> None:
        """Inicializa o serviço.

        Args:
            db_path: Caminho do arquivo SQLite.
        """
        self.db_path = db_path
        self.cap_repo = ContasAPagarMovRepository(db_path)

    # ------------------------------------------------------------------
    # Pagamento (principal pode cascatear; encargos não)
    # ------------------------------------------------------------------
    def pagar_emprestimo(
        self,
        *,
        obrigacao_id: int,
        # Aliases aceitos para o principal:
        principal: Optional[float] = None,
        valor_principal: Optional[float] = None,
        valor_base: Optional[float] = None,
        valor: Optional[float] = None,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        # Aliases de data
        data_evento: Optional[str] = None,
        data: Optional[str] = None,
        usuario: str = "-",
        # Log bancário direto (opcional)
        forma_pagamento: Optional[str] = None,
        origem: Optional[str] = None,
        ledger_id: Optional[int] = None,
        # Idempotência entre camadas (ex.: quando veio de Saída)
        trans_uid: Optional[str] = None,
        # Conexão opcional
        conn: Optional[sqlite3.Connection] = None,
    ) -> Dict[str, Any]:
        """Aplica o pagamento de EMPRESTIMO conforme regras FIFO e de encargos.

        Regras:
            - O `principal` é aplicado em cascata sobre as parcelas em aberto (FIFO).
            - `juros`, `multa` e `desconto` são aplicados apenas na PRIMEIRA parcela
              ainda em aberto no momento da operação (não cascateiam).

        Args:
            obrigacao_id: Identificador do grupo/parcelas a pagar (FIFO por vencimento).
            principal/valor_principal/valor_base/valor: Aliases para o valor de principal (>= 0).
            juros, multa, desconto: Encargos/abatimentos aplicados apenas na 1ª parcela aberta.
            data_evento/data: Data do evento no formato 'YYYY-MM-DD'. Padrão: hoje.
            usuario: Identificação do operador.
            forma_pagamento, origem, ledger_id: Para registrar a movimentação bancária quando chamado
                diretamente (ledger_id=None). Se veio do fluxo de SAÍDA, **não** registramos MB aqui.
            trans_uid: UID idempotente externo (preservado quando fornecido).
            conn: Conexão SQLite opcional. Se omitida, o serviço gerencia a transação.

        Returns:
            Dict com:
                ok (bool), trans_uid (str), saida_total (float), resultados (list[dict]),
                sobra (float), ids{ledger_id, cap_evento_id, mov_id}, e (opcional) mensagem.
        """
        # Normalizações de entrada
        _principal = (
            principal if principal is not None
            else (valor_principal if valor_principal is not None
                  else (valor_base if valor_base is not None else (valor or 0.0)))
        )
        data_evt = data_evento or data or datetime.now().strftime("%Y-%m-%d")
        _usuario = usuario or "-"

        # Padrões para possível log direto
        forma = (forma_pagamento or "DINHEIRO").upper()
        if forma == "DEBITO":
            forma = "DÉBITO"
        _origem = origem or ("Caixa" if forma == "DINHEIRO" else "Banco 1")

        # Core de pagamento
        core = self._pagar_core(
            obrigacao_id=int(obrigacao_id),
            principal=float(_principal or 0.0),
            juros=float(juros or 0.0),
            multa=float(multa or 0.0),
            desconto=float(desconto or 0.0),
            data_evento=data_evt,
            usuario=_usuario,
            conn=conn,
            trans_uid_override=trans_uid,
        )

        mov_id: Optional[int] = None
        # Se NÃO veio do fluxo de SAÍDA (sem ledger_id), registramos a movimentação aqui.
        # Usa log central com idempotência por trans_uid (não duplica caso já exista).
        if ledger_id is None and core["saida_total"] > 0:
            with self._conn_ctx(conn) as c:
                obs = _fmt_obs_saida(
                    forma=forma,
                    valor=float(core["saida_total"]),
                    categoria="EMPRESTIMO",
                    subcategoria=None,
                    descricao=f"Pagamento empréstimo (obrigação {obrigacao_id})",
                    banco=(_origem if forma == "DÉBITO" else None),
                )
                mov_id = log_mov_bancaria(
                    c,
                    data=data_evt,
                    banco=str(_origem or ""),
                    tipo="saida",
                    valor=float(core["saida_total"]),
                    origem="saidas",
                    observacao=obs,
                    usuario=_usuario,
                    referencia_id=int(obrigacao_id),
                    referencia_tabela="emprestimo",
                    trans_uid=str(core["trans_uid"]),
                    data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )

        resp: Dict[str, Any] = {
            "ok": True,
            "obrigacao_id": int(obrigacao_id),
            "sobra": float(core["sobra"]),
            "trans_uid": core["trans_uid"],
            "saida_total": float(core["saida_total"]),
            "resultados": core["resultados"],
            "ids": {
                "ledger_id": ledger_id if ledger_id is not None else None,
                "cap_evento_id": None,
                "mov_id": mov_id if (ledger_id is None and core["saida_total"] > 0) else None,
            },
        }
        if core.get("mensagem"):
            resp["ok"] = False
            resp["mensagem"] = core["mensagem"]
        return resp

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
        data_evt = data_evento or datetime.now().strftime("%Y-%m-%d")
        trans_uid = (
            trans_uid_override
            if trans_uid_override
            else f"EMPRESTIMO-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
        )

        # Sanitização (não permitir negativos)
        restante_principal = max(0.0, float(principal or 0.0))
        juros = max(0.0, float(juros or 0.0))
        multa = max(0.0, float(multa or 0.0))
        desconto = max(0.0, float(desconto or 0.0))

        resultados: List[ResultadoParcela] = []
        saida_total_agregado = 0.0

        with self._conn_ctx(conn) as c:
            # 1) Parcelas em aberto (FIFO) da obrigação
            fifo = self.cap_repo.listar_parcelas_em_aberto_fifo(c, obrigacao_id=obrigacao_id)
            if not fifo:
                return {
                    "trans_uid": trans_uid,
                    "saida_total": 0.0,
                    "resultados": [],
                    "sobra": float(restante_principal),
                    "mensagem": "Nenhuma parcela em aberto para esta obrigação.",
                }

            # 2) Encargos apenas na 1ª parcela
            encargos_pend = {"juros": juros, "multa": multa, "desconto": desconto}
            primeira = True

            for p in fifo:
                # Se não há principal a aplicar e a 1ª parcela já recebeu encargos, encerra
                if restante_principal <= _EPS and not primeira:
                    break

                parcela_id = int(p["parcela_id"])
                faltante_parcela = float(p["principal_faltante"] or 0.0)

                aplicar_principal = min(restante_principal, max(0.0, faltante_parcela))
                # Atualiza o restante do principal (2 casas para consistência monetária)
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

                # Se nada mais a aplicar e nada foi aplicado nesta iteração, encerra
                if restante_principal <= _EPS and aplicar_principal <= _EPS:
                    break

        return {
            "trans_uid": trans_uid,
            "saida_total": float(saida_total_agregado),
            "resultados": [r.__dict__ for r in resultados],
            "sobra": float(restante_principal),
        }

    # ------------------------------------------------------------------
    # Programação (criar N parcelas)
    # ------------------------------------------------------------------
    def programar_emprestimo(
        self,
        *,
        credor: str,
        data_primeira_parcela: str,
        parcelas_total: int,
        valor_parcela: float,
        usuario: str,
        descricao: Optional[str] = None,
        emprestimo_id: Optional[int] = None,
        conn: Optional[sqlite3.Connection] = None,
    ) -> List[int]:
        """Cria N lançamentos no CAP para o empréstimo/financiamento.

        Define:
            - tipo_obrigacao='EMPRESTIMO'
            - status inicial 'EM ABERTO'

        Args:
            credor: Nome do credor.
            data_primeira_parcela: Data da primeira parcela (YYYY-MM-DD).
            parcelas_total: Quantidade total de parcelas (>= 1).
            valor_parcela: Valor de cada parcela (> 0).
            usuario: Usuário operador.
            descricao: Descrição base opcional para as parcelas.
            emprestimo_id: Identificador externo/relacional opcional.
            conn: Conexão SQLite opcional. Se omitida, o serviço gerencia a transação.

        Returns:
            Lista de IDs dos lançamentos criados em `contas_a_pagar_mov`.
        """
        if int(parcelas_total) < 1:
            raise ValueError("Quantidade de parcelas inválida.")
        if float(valor_parcela) <= 0:
            raise ValueError("Valor da parcela deve ser > 0.")

        venc1 = pd.to_datetime(data_primeira_parcela)

        ids_cap: List[int] = []
        with self._conn_ctx(conn) as c:
            cur = c.cursor()

            # Base para obrigacao_id (compatível com o projeto)
            try:
                base_obrig_id = int(self.cap_repo.proximo_obrigacao_id(c))
            except Exception:
                row_max = cur.execute("SELECT COALESCE(MAX(obrigacao_id), 0) FROM contas_a_pagar_mov").fetchone()
                base_obrig_id = int((row_max[0] or 0) + 1)

            for p in range(1, int(parcelas_total) + 1):
                vcto = (venc1 + pd.DateOffset(months=p - 1)).date()
                obrig_id = base_obrig_id + (p - 1)

                desc_base = (descricao or f"{credor or 'Credor'} {p}/{int(parcelas_total)} - Empréstimo").strip()

                lanc_id = self.cap_repo.registrar_lancamento(
                    c,
                    obrigacao_id=obrig_id,
                    tipo_obrigacao="EMPRESTIMO",
                    valor_total=float(valor_parcela),
                    data_evento=str(vcto),       # contratação pode ser vcto da 1ª parcela
                    vencimento=str(vcto),
                    descricao=desc_base,
                    credor=credor,
                    competencia=str(vcto)[:7],   # YYYY-MM
                    parcela_num=p,
                    parcelas_total=int(parcelas_total),
                    usuario=usuario,
                    emprestimo_id=emprestimo_id,
                )
                ids_cap.append(int(lanc_id))

            # Marcar origem/status
            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET tipo_origem='EMPRESTIMO',
                       cartao_id=NULL,
                       status = CASE
                                  WHEN COALESCE(NULLIF(status,''), '') = ''
                                  THEN 'EM ABERTO' ELSE UPPER(status)
                                END
                 WHERE obrigacao_id BETWEEN ? AND ?
                   AND categoria_evento = 'LANCAMENTO'
                """,
                (base_obrig_id, base_obrig_id + int(parcelas_total) - 1),
            )

        return ids_cap

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


# Compatibilidade com código legado que importava um "mixin"
_EmprestimoLedgerMixin = ServiceLedgerEmprestimo
__all__ = ["ServiceLedgerEmprestimo", "_EmprestimoLedgerMixin", "ResultadoParcela"]
