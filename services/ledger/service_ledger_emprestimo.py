"""
Empréstimos (programação e pagamento).

Regras para programar parcelas de empréstimos em CAP e efetuar pagamento de
parcelas (dinheiro ou banco), aplicando multa/juros/desconto e atualizando
saldos/registros financeiros.

Responsabilidades:
- Programar N parcelas (`tipo_obrigacao='EMPRESTIMO'`) com competência/vencimento.
- Pagar parcela de empréstimo e registrar efeitos em `saida`/`movimentacoes_bancarias`.
- Atualizar status dos LANCAMENTOS e aplicar detalhamento do pagamento em CAP:
    - QUITAÇÃO TOTAL sem resíduo via `aplicar_pagamento_parcela_quitacao_total(...)`.

Dependências:
- shared.db.get_conn
- shared.ids.sanitize
- self.cap_repo (proximo_obrigacao_id, registrar_lancamento,
                 aplicar_pagamento_parcela_quitacao_total, obter_saldo_obrigacao)
- self._garantir_linha_saldos_caixas, self._garantir_linha_saldos_bancos,
  self._ajustar_banco_dynamic (validação/ajuste de colunas de bancos)

Notas de segurança:
- SQL apenas com parâmetros (?).
- Atualização de saldos de caixa com coluna whitelist (sem interpolar entrada do usuário).
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

# Garante que a raiz do projeto (<raiz>/services/ledger/..) esteja no sys.path
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Terceiros
import pandas as pd  # noqa: E402

# Internos
from shared.db import get_conn  # noqa: E402
from shared.ids import sanitize  # noqa: E402
from services.ledger.service_ledger_infra import (  # noqa: E402
    _fmt_obs_saida,
    log_mov_bancaria,
)

logger = logging.getLogger(__name__)

__all__ = ["_EmprestimoLedgerMixin"]


class _EmprestimoLedgerMixin:
    """Mixin com regras para empréstimos (pagar parcela e programar)."""

    # ------------------------------------------------------------------
    # Pagamento de parcela de empréstimo (QUITACAO TOTAL — sem resíduo)
    # ------------------------------------------------------------------
    def pagar_parcela_emprestimo(
        self,
        *,
        data: str,
        valor: float,
        forma_pagamento: str,   # "DINHEIRO" | "PIX" | "DÉBITO"
        origem: str,            # "Caixa"/"Caixa 2" ou nome do banco
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
        Mesmo padrão de Boleto/Fatura:
        - Calcula saída líquida: principal limitado ao saldo + juros + multa − desconto (mín. 0).
        - Se houver desembolso, gera saída (caixa/banco), log e ajusta saldos.
        - Em seguida, chama `aplicar_pagamento_parcela_quitacao_total(...)` para:
            * distribuir juros/multa/desconto,
            * definir valor_pago_acumulado = base + juros + multa,
            * status = 'QUITADO',
            * registrar evento CAP negativo com o valor efetivamente pago (se > 0).
        - Se a saída líquida for 0 (ex.: desconto cobre tudo), não mexe em caixa/banco;
          ainda assim a parcela fica QUITADA sem gerar “resto”.
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

        eps = 0.005  # tolerância p/ ponto flutuante

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # 1) Parcela base (LANCAMENTO)
            row = cur.execute(
                """
                SELECT id, COALESCE(valor_evento,0) AS valor_parcela
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

            # 2) Saldo atual e cálculo da saída
            saldo_atual = float(self.cap_repo.obter_saldo_obrigacao(conn, int(obrigacao_id)) or 0.0)
            principal_a_pagar = min(v_pg, max(saldo_atual, 0.0))
            total_saida = round(principal_a_pagar + v_juros + v_multa - v_desc, 2)
            if total_saida < 0:
                total_saida = 0.0

            id_saida = -1
            id_mov   = -1

            # 3) Se houver desembolso, saída + saldos + log
            if total_saida > eps:
                obs = _fmt_obs_saida(
                    forma=forma_pagamento,
                    valor=float(total_saida),
                    categoria=cat,
                    subcategoria=sub,
                    descricao=desc,
                    banco=(org if forma_pagamento == "DÉBITO" else None),
                )

                if forma_pagamento == "DINHEIRO":
                    self._garantir_linha_saldos_caixas(conn, data)
                    col_map = {"Caixa": "caixa", "Caixa 2": "caixa_2"}  # whitelist
                    col = col_map.get(org)
                    if not col:
                        raise ValueError(f"Origem de dinheiro inválida para DINHEIRO: {org}")

                    # (1) Saída (LÍQUIDO)
                    cur.execute(
                        """
                        INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                           Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                           Origem_Dinheiro, Banco_Saida)
                        VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
                        """,
                        (data, cat, sub, desc, float(total_saida), usu, org),
                    )
                    id_saida = int(cur.lastrowid)

                    # (2) Atualiza saldo do caixa
                    cur.execute(
                        f"UPDATE saldos_caixas SET {col} = COALESCE({col},0) - ? WHERE data = ?",
                        (float(total_saida), data),
                    )

                    # (3) Log
                    id_mov = log_mov_bancaria(
                        conn,
                        data=data,
                        banco=org,  # "Caixa"/"Caixa 2"
                        tipo="saida",
                        valor=float(total_saida),    # LÍQUIDO
                        origem="saidas_emprestimo_pagamento",
                        observacao=obs,
                        usuario=usu,
                        referencia_tabela="saida",
                        referencia_id=id_saida,
                        trans_uid=trans_uid,
                        data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )
                else:
                    # Bancos
                    try:
                        self._garantir_linha_saldos_bancos(conn, data)
                    except Exception:
                        pass

                    self._ajustar_banco_dynamic(conn, banco_col=org, delta=-float(total_saida), data=data)

                    cur.execute(
                        """
                        INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                           Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                           Origem_Dinheiro, Banco_Saida)
                        VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
                        """,
                        (data, cat, sub, desc, forma_pagamento, float(total_saida), usu, org),
                    )
                    id_saida = int(cur.lastrowid)

                    id_mov = log_mov_bancaria(
                        conn,
                        data=data,
                        banco=org,                      # nome do banco
                        tipo="saida",
                        valor=float(total_saida),       # LÍQUIDO
                        origem="saidas_emprestimo_pagamento",
                        observacao=obs,
                        usuario=usu,
                        referencia_tabela="saida",
                        referencia_id=id_saida,
                        trans_uid=trans_uid,
                        data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                # Observação extra se informou base acima do saldo e foi ajustado
                if v_pg > saldo_atual + eps:
                    cur.execute(
                        """
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [valor ajustado ao saldo: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                        """,
                        (float(total_saida), id_mov),
                    )

            # 4) QUITAÇÃO TOTAL — sempre (com ou sem desembolso)
            res = self.cap_repo.aplicar_pagamento_parcela_quitacao_total(
                conn,
                parcela_id=int(parcela_id),
                juros=float(v_juros),
                multa=float(v_multa),
                desconto=float(v_desc),
                data_evento=data,
                forma_pagamento=forma_pagamento,
                origem=org,
                ledger_id=(id_saida if id_saida != -1 else 0),
                usuario=usu,
            )
            evento_id = int(res.get("id_evento_cap", -1))

            conn.commit()

        logger.debug(
            "pagar_parcela_emprestimo: obrig=%s total=%.2f base=%.2f forma=%s origem=%s saida=%s mov=%s evento=%s",
            obrigacao_id,
            total_saida,
            valor_parcela,
            forma_pagamento,
            org,
            id_saida,
            id_mov,
            evento_id,
        )
        return (id_saida, id_mov, evento_id)

    # ------------------------------------------------------------------
    # Programação de empréstimo (N parcelas)
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

            base_obrig_id = self.cap_repo.proximo_obrigacao_id(conn)

            for i in range(1, int(parcelas_total) + 1):
                vcto = (base_dt + pd.DateOffset(months=i - 1)).date()
                competencia = f"{vcto.year:04d}-{vcto.month:02d}"
                obrigacao_id = base_obrig_id + (i - 1)

                lanc_id = self.cap_repo.registrar_lancamento(
                    conn,
                    obrigacao_id=obrigacao_id,
                    tipo_obrigacao="EMPRESTIMO",
                    valor_total=float(valor_parcela),
                    data_evento=str(base_dt.date()),
                    vencimento=str(vcto),
                    descricao=desc or f"{credor} {i}/{parcelas_total}",
                    credor=credor,
                    competencia=competencia,
                    parcela_num=i,
                    parcelas_total=int(parcelas_total),
                    usuario=usuario,
                )
                ids_lanc.append(int(lanc_id))

            # Marca origem/status e liga ao cadastro do empréstimo (se houver)
            if emprestimo_id is not None:
                try:
                    emprestimo_id = int(emprestimo_id)
                except Exception:
                    emprestimo_id = None

            if emprestimo_id is not None:
                cur.execute(
                    """
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='EMPRESTIMO',
                           emprestimo_id=?,
                           cartao_id=NULL,
                           status = COALESCE(NULLIF(status,''), 'Em aberto')
                     WHERE obrigacao_id BETWEEN ? AND ?
                    """,
                    (int(emprestimo_id), base_obrig_id, base_obrig_id + int(parcelas_total) - 1),
                )
            else:
                cur.execute(
                    """
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='EMPRESTIMO',
                           cartao_id=NULL,
                           status = COALESCE(NULLIF(status,''), 'Em aberto')
                     WHERE obrigacao_id BETWEEN ? AND ?
                    """,
                    (base_obrig_id, base_obrig_id + int(parcelas_total) - 1),
                )

            # Registra parcelas já pagas (opcional)
            k = max(0, min(int(parcelas_ja_pagas or 0), int(parcelas_total)))
            if k > 0:
                for i in range(0, k):
                    obrig_id = base_obrig_id + i
                    row = cur.execute(
                        """
                        SELECT COALESCE(vencimento, data_evento) AS vcto
                          FROM contas_a_pagar_mov
                         WHERE obrigacao_id = ?
                           AND categoria_evento='LANCAMENTO'
                         LIMIT 1
                        """,
                        (obrig_id,),
                    ).fetchone()
                    vcto = row[0] if row and row[0] else str((base_dt + pd.DateOffset(months=i)).date())

                    # Para registros “já pagos” na origem, inserimos um evento CAP simples
                    ev_id = self.cap_repo.registrar_pagamento(
                        conn,
                        obrigacao_id=int(obrig_id),
                        tipo_obrigacao="EMPRESTIMO",
                        valor_pago=float(valor_parcela),
                        data_evento=str(vcto),
                        forma_pagamento="AJUSTE",
                        origem="programacao",
                        ledger_id=None,
                        usuario=usuario,
                    )
                    ids_pay.append(int(ev_id))

                cur.execute(
                    """
                    UPDATE contas_a_pagar_mov
                       SET status='Quitado'
                     WHERE obrigacao_id BETWEEN ? AND ? AND categoria_evento='LANCAMENTO'
                    """,
                    (base_obrig_id, base_obrig_id + k - 1),
                )

            # Log informativo de programação
            log_mov_bancaria(
                conn,
                data=str(base_dt.date()),
                banco=credor,
                tipo="info",
                valor=0.0,
                origem="emprestimo_programado",
                observacao=(
                    f"Empréstimo programado {parcelas_total}x de R$ {valor_parcela:.2f} - "
                    f"{credor} (pagas na origem: {k})"
                ),
                usuario=usuario,
                referencia_tabela="contas_a_pagar_mov",
                referencia_id=(ids_lanc[0] if ids_lanc else None),
            )

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
    # Programar a partir do cadastro de empréstimos
    # ------------------------------------------------------------------
    def programar_emprestimo_por_cadastro(
        self,
        *,
        cadastro: dict,
        usuario: str,
    ) -> Tuple[List[int], List[int]]:
        """Cria programação de empréstimo a partir de um dict (cadastro) existente."""
        credor = sanitize(
            cadastro.get("banco")
            or cadastro.get("descricao")
            or cadastro.get("tipo")
            or "Empréstimo"
        )

        data_primeira = str(cadastro.get("data_primeira_parcela") or "").strip()
        if not data_primeira:
            raise ValueError("data_primeira_parcela ausente no cadastro.")

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
            pp = int(cadastro.get("parcelas_ja_pagas") or 0)
        except Exception:
            pp = 0

        emp_id = cadastro.get("id")
        try:
            emp_id = int(emp_id) if emp_id is not None else None
        except Exception:
            emp_id = None

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
