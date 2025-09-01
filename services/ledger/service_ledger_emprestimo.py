# services/ledger/service_ledger_emprestimo.py
"""
Empréstimos (programação e pagamento de parcelas).

Este módulo padroniza o pagamento de **parcela de empréstimo/financiamento**
para o mesmo modelo adotado em Fatura e Boleto:

- O parâmetro `valor` é o **valor BASE (principal)** que será aplicado no saldo da parcela.
- A saída financeira (caixa/banco) é calculada como:
    total_saida = principal_aplicado + juros + multa - desconto  (mínimo 0)
- O lançamento contábil e o ajuste de saldos só ocorrem quando `total_saida > 0`.
- A aplicação no CAP é feita **sempre via `aplicar_pagamento_parcela`** (parcial),
  passando `valor_base`, `juros`, `multa`, `desconto` e metadados (`forma_pagamento`,
  `origem`, `ledger_id`, `usuario`). O mixin de pagamentos decide `status/restante`.

Também expõe utilitários para **programar** um empréstimo em N parcelas.

Dependências (expostas pela service/fachada que mistura este mixin):
- self.db_path
- self.cap_repo (proximo_obrigacao_id, registrar_lancamento, aplicar_pagamento_parcela, registrar_pagamento, obter_saldo_obrigacao)
- self.mov_repo (ja_existe_transacao) [opcional]
- self._garantir_linha_saldos_caixas, self._garantir_linha_saldos_bancos, self._ajustar_banco_dynamic
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import logging
import os
import sys
from datetime import datetime
from typing import List, Optional, Tuple

# Garante que a raiz do projeto esteja no sys.path (../.. a partir deste arquivo)
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pandas as pd  # noqa: E402

from shared.db import get_conn  # noqa: E402
from shared.ids import sanitize  # noqa: E402
from services.ledger.service_ledger_infra import (  # noqa: E402
    _fmt_obs_saida,
    log_mov_bancaria,
)

logger = logging.getLogger(__name__)


class EmprestimoLedgerMixin:
    """Mixin com regras para Empréstimos (pagar parcela e programar)."""

    # ------------------------------------------------------------------
    # Pagamento de parcela de empréstimo (PARCIAL/TOTAL via aplicar_pagamento_parcela)
    # ------------------------------------------------------------------
    def pagar_parcela_emprestimo(
        self,
        *,
        data: str,
        valor: float,                 # valor BASE (principal) a aplicar na parcela
        forma_pagamento: str,         # "DINHEIRO" | "PIX" | "DÉBITO"
        origem: str,                  # "Caixa"/"Caixa 2" ou nome do banco
        obrigacao_id: int,
        usuario: str,
        categoria: Optional[str] = "Empréstimos e Financiamentos",
        sub_categoria: Optional[str] = None,
        descricao: Optional[str] = None,
        trans_uid: Optional[str] = None,
        multa: float = 0.0,
        juros: float = 0.0,
        desconto: float = 0.0,
    ) -> Tuple[int, int, int]:
        """
        Aplica pagamento de parcela de **empréstimo/financiamento** no mesmo padrão de Fatura/Boleto.

        - `valor` é aplicado no **principal** até o limite do restante.
        - Saída financeira (quando > 0): `principal_aplicado + juros + multa - desconto`.
        - Aplica no CAP com `aplicar_pagamento_parcela` (parcial). O mixin define `status/restante`.

        Retorna `(id_saida, id_mov_bancaria, id_evento_cap)`. Em caso de idempotência, retorna `(-1, -1, -1)`.
        """
        v_pg    = max(0.0, float(valor))
        v_multa = max(0.0, float(multa or 0.0))
        v_juros = max(0.0, float(juros or 0.0))
        v_desc  = max(0.0, float(desconto or 0.0))

        cat  = sanitize(categoria)
        sub  = sanitize(sub_categoria)
        desc = sanitize(descricao)
        usu  = sanitize(usuario)
        org  = sanitize(origem)

        eps = 0.005  # tolerância de ponto flutuante

        # Idempotência por trans_uid (se fornecido)
        try:
            if trans_uid and hasattr(self, "mov_repo") and self.mov_repo.ja_existe_transacao(trans_uid):
                logger.info("pagar_parcela_emprestimo: trans_uid já existe (%s) — ignorando", trans_uid)
                return (-1, -1, -1)
        except Exception:
            pass

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # 1) Ler a parcela-base (LANCAMENTO) p/ obter 'valor_evento' e 'valor_pago_acumulado'
            row = cur.execute(
                """
                SELECT id,
                       COALESCE(valor_evento,0)           AS valor_parcela,
                       COALESCE(valor_pago_acumulado,0)   AS pago_acum,
                       UPPER(COALESCE(status,''))         AS status_atual
                  FROM contas_a_pagar_mov
                 WHERE obrigacao_id = ?
                   AND categoria_evento = 'LANCAMENTO'
                 LIMIT 1
                """,
                (int(obrigacao_id),),
            ).fetchone()
            if not row:
                raise ValueError(f"Parcela (obrigacao_id={obrigacao_id}) não encontrada.")

            parcela_id    = int(row[0])
            valor_parcela = float(row[1])
            pago_acum     = float(row[2])
            status_atual  = (row[3] or "").upper()

            # Se já quitada, não repete
            if status_atual == "QUITADA" or (valor_parcela - pago_acum) <= eps:
                logger.info("pagar_parcela_emprestimo: parcela já quitada (obrigacao_id=%s).", obrigacao_id)
                return (-1, -1, -1)

            # 2) RESTANTE e clamp do principal BASE
            restante_antes     = max(0.0, valor_parcela - pago_acum)
            principal_a_pagar  = min(v_pg, restante_antes)

            # 3) LÍQUIDO que sai do caixa/banco
            total_saida = round(principal_a_pagar + v_juros + v_multa - v_desc, 2)
            if total_saida < 0:
                total_saida = 0.0

            id_saida = -1
            id_mov   = -1

            # 4) Observação padronizada (usa o valor efetivo desembolsado)
            obs = _fmt_obs_saida(
                forma=(forma_pagamento if total_saida > eps else "AJUSTE"),
                valor=float(total_saida if total_saida > eps else 0.0),
                categoria=cat,
                subcategoria=sub,
                descricao=desc,
                banco=(org if (total_saida > eps and forma_pagamento == "DÉBITO") else None),
            )

            # 5) Efeito financeiro (saida/saldos) e log — somente se houver desembolso
            if total_saida > eps:
                data_iso = str(pd.to_datetime(data).date())
                if forma_pagamento == "DINHEIRO":
                    self._garantir_linha_saldos_caixas(conn, data_iso)
                    col_map = {"Caixa": "caixa", "Caixa 2": "caixa_2"}  # whitelist
                    col = col_map.get(org)
                    if not col:
                        raise ValueError(f"Origem de dinheiro inválida para DINHEIRO: {org}")

                    # Saída (DINHEIRO)
                    cur.execute(
                        """
                        INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                           Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                           Origem_Dinheiro, Banco_Saida)
                        VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
                        """,
                        (data_iso, cat, sub, desc, float(total_saida), usu, org),
                    )
                    id_saida = int(cur.lastrowid)

                    # Atualiza saldo do caixa
                    cur.execute(
                        f"UPDATE saldos_caixas SET {col} = COALESCE({col},0) - ? WHERE data = ?",
                        (float(total_saida), data_iso),
                    )

                    # Log financeiro
                    id_mov = log_mov_bancaria(
                        conn,
                        data=data_iso,
                        banco=org,  # "Caixa" / "Caixa 2"
                        tipo="saida",
                        valor=float(total_saida),    # TOTAL (principal + ajustes)
                        origem="saidas_emprestimo_pagamento",
                        observacao=obs,
                        usuario=usu,
                        referencia_tabela="saida",
                        referencia_id=id_saida,
                        trans_uid=trans_uid,
                        data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )
                else:
                    # Bancos (PIX/DÉBITO)
                    try:
                        self._garantir_linha_saldos_bancos(conn, data_iso)
                    except Exception:
                        pass

                    self._ajustar_banco_dynamic(conn, banco_col=org, delta=-float(total_saida), data=data_iso)

                    # Saída (Banco)
                    cur.execute(
                        """
                        INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                           Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                           Origem_Dinheiro, Banco_Saida)
                        VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
                        """,
                        (data_iso, cat, sub, desc, forma_pagamento, float(total_saida), usu, org),
                    )
                    id_saida = int(cur.lastrowid)

                    # Log financeiro
                    id_mov = log_mov_bancaria(
                        conn,
                        data=data_iso,
                        banco=org,                      # nome do banco
                        tipo="saida",
                        valor=float(total_saida),       # TOTAL (principal + ajustes)
                        origem="saidas_emprestimo_pagamento",
                        observacao=obs,
                        usuario=usu,
                        referencia_tabela="saida",
                        referencia_id=id_saida,
                        trans_uid=trans_uid,
                        data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                # Observação extra no log se o valor BASE informado foi ajustado ao restante
                if abs(v_pg - principal_a_pagar) > eps and id_mov != -1:
                    cur.execute(
                        """
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [valor base ajustado ao restante: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                        """,
                        (float(principal_a_pagar), id_mov),
                    )

            # 6) Aplicar no CAP — sempre via PARCIAL
            evento_id = -1
            if hasattr(self.cap_repo, "aplicar_pagamento_parcela"):
                res = self.cap_repo.aplicar_pagamento_parcela(
                    conn,
                    parcela_id=int(parcela_id),
                    valor_base=float(principal_a_pagar),
                    juros=float(v_juros),
                    multa=float(v_multa),
                    desconto=float(v_desc),
                    data_evento=str(pd.to_datetime(data).date()),
                    forma_pagamento=forma_pagamento,
                    origem=org,
                    ledger_id=(id_saida if id_saida != -1 else 0),
                    usuario=usu,
                )
                try:
                    evento_id = int(res.get("id_evento_cap", -1)) if isinstance(res, dict) else int(res or -1)
                except Exception:
                    evento_id = -1
            else:
                # Fallback SQL (compatibilidade) — aplica principal e acumuladores
                cur2 = conn.cursor()
                cur2.execute("PRAGMA table_info('contas_a_pagar_mov')")
                cols = [r[1] for r in cur2.fetchall()]
                multa_col = "multa_paga" if "multa_paga" in cols else ("multa_pago" if "multa_pago" in cols else None)
                desc_col  = "desconto_aplicado" if "desconto_aplicado" in cols else ("desconto" if "desconto" in cols else None)

                sets = [
                    "valor_pago_acumulado = COALESCE(valor_pago_acumulado,0) + :principal",
                    "juros_pago = COALESCE(juros_pago,0) + :juros",
                    "data_pagamento = DATE(:data_evento)",
                    "status = CASE WHEN COALESCE(valor_evento,0) <= COALESCE(valor_pago_acumulado,0) + :principal THEN 'QUITADA' ELSE 'EM ABERTO' END",
                ]
                params = {
                    "principal": float(principal_a_pagar),
                    "juros": float(v_juros),
                    "data_evento": str(pd.to_datetime(data).date()),
                    "parcela_id": int(parcela_id),
                }
                if multa_col:
                    sets.append(f"{multa_col} = COALESCE({multa_col},0) + :multa")
                    params["multa"] = float(v_multa)
                if desc_col:
                    sets.append(f"{desc_col} = COALESCE({desc_col},0) + :desconto")
                    params["desconto"] = float(v_desc)

                cur2.execute(
                    f"UPDATE contas_a_pagar_mov SET {', '.join(sets)} WHERE id = :parcela_id",
                    params,
                )
                evento_id = -1

            conn.commit()

        logger.debug(
            "pagar_parcela_emprestimo: obrig=%s total_saida=%.2f principal=%.2f forma=%s origem=%s saida=%s mov=%s evento=%s",
            obrigacao_id,
            total_saida,
            principal_a_pagar,
            forma_pagamento,
            org,
            id_saida,
            id_mov,
            evento_id,
        )
        return (id_saida, id_mov, evento_id)

    # ------------------------------------------------------------------
    # Programar empréstimo (N parcelas de mesmo valor)
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
        parcelas_ja_pagas: int = 0,
    ) -> Tuple[List[int], List[int]]:
        """Programa um empréstimo em `parcelas_total` parcelas de `valor_parcela`."""
        credor = sanitize(credor or "").strip() or "Empréstimo"
        usuario = sanitize(usuario or "Sistema")
        desc = sanitize(descricao)

        data_primeira_parcela = str(data_primeira_parcela or "").strip()
        if not data_primeira_parcela:
            raise ValueError("data_primeira_parcela não informada.")

        try:
            base_dt = pd.to_datetime(data_primeira_parcela)
        except Exception:
            raise ValueError(f"data_primeira_parcela inválida: {data_primeira_parcela!r}")

        try:
            parcelas_total = int(parcelas_total or 0)
        except Exception:
            parcelas_total = 0

        try:
            valor_parcela = float(valor_parcela or 0.0)
        except Exception:
            valor_parcela = 0.0

        try:
            parcelas_ja_pagas = int(parcelas_ja_pagas or 0)
        except Exception:
            parcelas_ja_pagas = 0

        if parcelas_total < 1:
            raise ValueError("parcelas_total deve ser >= 1")
        if valor_parcela <= 0:
            raise ValueError("valor_parcela deve ser > 0")

        ids_lanc: List[int] = []
        ids_pay: List[int] = []

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # Base para obrigacao_id
            if hasattr(self.cap_repo, "proximo_obrigacao_id"):
                base_obrig_id = int(self.cap_repo.proximo_obrigacao_id(conn))
            else:
                row_max = cur.execute("SELECT COALESCE(MAX(obrigacao_id), 0) FROM contas_a_pagar_mov").fetchone()
                base_obrig_id = int((row_max[0] or 0) + 1)

            # 1) Gerar LANCAMENTOS
            for i in range(parcelas_total):
                obrig_id = base_obrig_id + i
                vcto = (base_dt + pd.DateOffset(months=i)).date()
                descricao_cap = (desc or f"{credor} {i+1}/{parcelas_total} - Empréstimo")

                lanc_id = self.cap_repo.registrar_lancamento(
                    conn,
                    obrigacao_id=int(obrig_id),
                    tipo_obrigacao="EMPRESTIMO",
                    valor_total=float(valor_parcela),
                    data_evento=str(vcto),       # registra na competência
                    vencimento=str(vcto),
                    descricao=descricao_cap,
                    credor=credor,
                    competencia=str(vcto)[:7],   # YYYY-MM
                    parcela_num=i+1,
                    parcelas_total=parcelas_total,
                    usuario=usuario,
                )
                ids_lanc.append(int(lanc_id))

            # Marcar origem e status padrão
            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET tipo_origem='EMPRESTIMO',
                       cartao_id=NULL,
                       status = CASE WHEN COALESCE(NULLIF(status,''), '') = '' THEN 'EM ABERTO' ELSE UPPER(status) END
                 WHERE obrigacao_id BETWEEN ? AND ?
                """,
                (base_obrig_id, base_obrig_id + int(parcelas_total) - 1),
            )

            # 2) Opcional: marcar K primeiras parcelas como já pagas (origem)
            k = max(0, min(int(parcelas_ja_pagas or 0), int(parcelas_total)))
            if k > 0:
                for i in range(k):
                    obrig_id = base_obrig_id + i
                    vcto = (base_dt + pd.DateOffset(months=i)).date()
                    ev_id = self.cap_repo.registrar_pagamento(
                        conn,
                        obrigacao_id=int(obrig_id),
                        tipo_obrigacao="EMPRESTIMO",
                        valor_total=float(valor_parcela),
                        data_evento=str(vcto),
                        descricao="Pagamento já realizado na contratação",
                        usuario=usuario,
                    )
                    ids_pay.append(int(ev_id))

            conn.commit()

        logger.debug(
            "programar_emprestimo: credor=%s parcelas=%s valor=%.2f base_obrig=%s pagas_iniciais=%s",
            credor,
            parcelas_total,
            valor_parcela,
            base_obrig_id,
            len(ids_pay),
        )
        return (ids_lanc, ids_pay)

    # ------------------------------------------------------------------
    # Programar a partir de um cadastro (dict) de empréstimos
    # ------------------------------------------------------------------
    def programar_emprestimo_por_cadastro(
        self,
        *,
        cadastro: dict,
        usuario: str,
    ) -> Tuple[List[int], List[int]]:
        """Cria programação de empréstimo a partir de um dict `cadastro` existente."""
        credor = sanitize(
            cadastro.get("banco")
            or cadastro.get("descricao")
            or cadastro.get("tipo")
            or "Empréstimo"
        )

        data_primeira = str(cadastro.get("data_primeira_parcela") or "").strip()
        if not data_primeira:
            raise ValueError("data_primeira_parcela não informada no cadastro.")

        try:
            pd.to_datetime(data_primeira)
        except Exception:
            raise ValueError(f"data_primeira_parcela inválida: {data_primeira!r}")

        try:
            pt = int(cadastro.get("parcelas_total") or 0)
        except Exception:
            pt = 0

        try:
            vp = float(cadastro.get("valor_parcela") or 0.0)
        except Exception:
            vp = 0.0

        try:
            emp_id = int(cadastro.get("id") or cadastro.get("emprestimo_id") or 0)
        except Exception:
            emp_id = 0

        try:
            pp = int(cadastro.get("parcelas_ja_pagas") or 0)
        except Exception:
            pp = 0

        if pt < 1:
            raise ValueError(f"parcelas_total inválido: {cadastro.get('parcelas_total')!r}")
        if vp <= 0:
            raise ValueError(f"valor_parcela inválido: {cadastro.get('valor_parcela')!r}")

        return self.programar_emprestimo(
            credor=credor,
            data_primeira_parcela=data_primeira,
            parcelas_total=pt,
            valor_parcela=vp,
            usuario=usuario,
            descricao=cadastro.get("observacao"),
            emprestimo_id=emp_id,
            parcelas_ja_pagas=pp,
        )


# --- Alias de compatibilidade (alguns módulos importam _EmprestimoLedgerMixin) ---
_EmprestimoLedgerMixin = EmprestimoLedgerMixin
__all__ = ["EmprestimoLedgerMixin", "_EmprestimoLedgerMixin"]
