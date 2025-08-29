"""
service_ledger_emprestimo.py — Empréstimos (programação e pagamento).

Resumo:
    Regras para programar parcelas de empréstimos em CAP e efetuar pagamento
    de parcelas (dinheiro ou banco), aplicando multa/juros/desconto e atualizando
    saldos/regs financeiros.

Responsabilidades:
    - Programar N parcelas (tipo_obrigacao='EMPRESTIMO') com competência/vencimento.
    - Pagar parcela de empréstimo e registrar efeitos em saida/movimentacoes_bancarias.
    - Atualizar status dos LANCAMENTOS e aplicar detalhamento do pagamento em CAP.

Depende de:
    - shared.db.get_conn
    - shared.ids.sanitize
    - self.cap_repo (proximo_obrigacao_id, registrar_lancamento, registrar_pagamento,
                    aplicar_pagamento_parcela, obter_saldo_obrigacao)
    - self._garantir_linha_saldos_caixas, self._garantir_linha_saldos_bancos,
      self._ajustar_banco_dynamic (validação/ajuste de colunas de bancos)

Notas de segurança:
    - SQL apenas com parâmetros (?).
    - Atualização de saldos de caixa com coluna **whitelist** (sem interpolar entrada do usuário).
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Imports + bootstrap de caminho (robusto em execuções via Streamlit)
# -----------------------------------------------------------------------------
import logging
from typing import Optional, List, Tuple

import os
import sys

# Garante que a raiz do projeto (<raiz>/services/ledger/..) esteja no sys.path
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Terceiros
import pandas as pd  # noqa: E402
from datetime import datetime  # <-- para data_hora

# Internos
from shared.db import get_conn  # noqa: E402
from shared.ids import sanitize  # noqa: E402
from services.ledger.service_ledger_infra import _ensure_mov_cols  # <-- garante colunas usuario/data_hora

logger = logging.getLogger(__name__)

__all__ = ["_EmprestimoLedgerMixin"]


class _EmprestimoLedgerMixin:
    """Mixin com regras para empréstimos (pagar parcela e programar)."""

    # ----------------------------------------------------------------------
    # Pagamento de parcela de empréstimo
    # ----------------------------------------------------------------------
    def pagar_parcela_emprestimo(
        self,
        *,
        data: str,
        valor: float,
        forma_pagamento: str,
        origem: str,
        obrigacao_id: int,
        usuario: str,
        categoria: Optional[str] = "Empréstimos e Financiamentos",
        sub_categoria: Optional[str] = None,
        descricao: Optional[str] = None,
        trans_uid: Optional[str] = None,
        multa: float = 0.0,
        juros: float = 0.0,
        desconto: float = 0.0,
    ) -> tuple[int, int, int]:
        """
        Paga uma parcela de empréstimo (obrigacao_id) e registra efeitos financeiros.

        Retorna:
            (id_saida, id_mov_bancaria, id_evento_cap)
        """
        v_pg = max(0.0, float(valor))
        v_multa = max(0.0, float(multa or 0.0))
        v_juros = max(0.0, float(juros or 0.0))
        v_desc = max(0.0, float(desconto or 0.0))

        cat = sanitize(categoria)
        sub = sanitize(sub_categoria)
        desc = sanitize(descricao)
        usu = sanitize(usuario)
        org = sanitize(origem)

        eps = 0.005  # tolerância p/ ponto flutuante

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            saldo_atual = self.cap_repo.obter_saldo_obrigacao(conn, int(obrigacao_id))
            valor_a_pagar = min(v_pg, max(saldo_atual, 0.0))
            total_saida = max(round(valor_a_pagar + v_multa + v_juros - v_desc, 2), 0.0)

            # --- saída e ajuste de saldos
            if forma_pagamento == "DINHEIRO":
                self._garantir_linha_saldos_caixas(conn, data)
                # coluna validada via whitelist
                col_map = {"Caixa": "caixa", "Caixa 2": "caixa_2"}
                col = col_map.get(org)
                if not col:
                    raise ValueError(f"Origem de dinheiro inválida para DINHEIRO: {org}")

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

                # Atualiza coluna validada
                cur.execute(
                    f"UPDATE saldos_caixas SET {col} = COALESCE({col},0) - ? WHERE data = ?",
                    (float(total_saida), data),
                )

                # Log do pagamento em DINHEIRO — inclui usuario e data_hora
                _ensure_mov_cols(cur)
                obs = (
                    (f"Pagamento Empréstimo {cat}/{sub or ''}").strip()
                    + (f" - {desc}" if desc else "")
                    + (f" | multa R$ {v_multa:.2f}" if v_multa > 0 else "")
                    + (f", juros R$ {v_juros:.2f}" if v_juros > 0 else "")
                    + (f", desconto R$ {v_desc:.2f}" if v_desc > 0 else "")
                )
                cur.execute(
                    """
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao,
                         referencia_tabela, referencia_id, trans_uid, usuario, data_hora)
                    VALUES (?, ?, 'saida', ?, 'saidas_emprestimo_pagamento', ?, 'saida', ?, ?, ?, ?)
                    """,
                    (
                        data,
                        org,  # "Caixa" ou "Caixa 2"
                        float(total_saida),
                        obs,
                        id_saida,
                        trans_uid,
                        usu,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )
                id_mov = int(cur.lastrowid)

            else:
                # Banco: validação/ajuste dentro de _ajustar_banco_dynamic
                self._garantir_linha_saldos_bancos(conn, data)
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

                # Log do pagamento via banco — inclui usuario e data_hora
                _ensure_mov_cols(cur)
                obs = (
                    (f"Pagamento Empréstimo {cat}/{sub or ''}").strip()
                    + (f" - {desc}" if desc else "")
                    + (f" | multa R$ {v_multa:.2f}" if v_multa > 0 else "")
                    + (f", juros R$ {v_juros:.2f}" if v_juros > 0 else "")
                    + (f", desconto R$ {v_desc:.2f}" if v_desc > 0 else "")
                )
                cur.execute(
                    """
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao,
                         referencia_tabela, referencia_id, trans_uid, usuario, data_hora)
                    VALUES (?, ?, 'saida', ?, 'saidas_emprestimo_pagamento', ?, 'saida', ?, ?, ?, ?)
                    """,
                    (
                        data,
                        org,  # nome do banco
                        float(total_saida),
                        obs,
                        id_saida,
                        trans_uid,
                        usu,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )
                id_mov = int(cur.lastrowid)

            # --- CAP: evento de pagamento + detalhamento
            evento_id = self.cap_repo.registrar_pagamento(
                conn,
                obrigacao_id=int(obrigacao_id),
                tipo_obrigacao="EMPRESTIMO",
                valor_pago=float(total_saida),
                data_evento=data,
                forma_pagamento=forma_pagamento,
                origem=org,
                ledger_id=int(id_saida),
                usuario=usu,
            )

            # aplicar detalhamento na parcela (juros/multa/desconto), se houver LANCAMENTO
            try:
                row = conn.execute(
                    """
                    SELECT id, COALESCE(valor_evento,0) AS valor_parcela
                      FROM contas_a_pagar_mov
                     WHERE obrigacao_id = ?
                       AND categoria_evento = 'LANCAMENTO'
                     LIMIT 1
                    """,
                    (int(obrigacao_id),),
                ).fetchone()
                if row:
                    parcela_id = int(row[0])
                    valor_parcela = float(row[1])
                    self.cap_repo.aplicar_pagamento_parcela(
                        conn,
                        parcela_id=int(parcela_id),
                        valor_parcela=float(valor_parcela),
                        valor_pago_total=float(total_saida),
                        juros=float(v_juros),
                        multa=float(v_multa),
                        desconto=float(v_desc),
                    )
            except Exception as e:
                logger.exception("Falha ao aplicar detalhamento de pagamento emprestimo: %s", e)

            conn.commit()

        logger.debug(
            "pagar_parcela_emprestimo: obrig=%s total=%.2f forma=%s origem=%s saida=%s mov=%s evento=%s",
            obrigacao_id, total_saida, forma_pagamento, org, id_saida, id_mov, evento_id
        )
        return (id_saida, id_mov, int(evento_id))

    # ----------------------------------------------------------------------
    # Programação de empréstimo (N parcelas)
    # ----------------------------------------------------------------------
    def programar_emprestimo(
        self,
        *,
        credor: str,
        data_primeira_parcela: str,
        parcelas_total: int,
        valor_parcela: float,
        usuario: str,
        descricao: str | None = None,
        emprestimo_id: int | None = None,
        parcelas_ja_pagas: int = 0,
    ) -> tuple[list[int], list[int]]:
        """
        Programa um empréstimo em `parcelas_total` parcelas de `valor_parcela`.

        Retorna:
            (ids_lancamentos_cap, ids_pagamentos_iniciais)
        """
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

        ids_lanc: list[int] = []
        ids_pay: list[int] = []

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

            # marca origem/status e liga ao cadastro do empréstimo (se houver)
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

            # registra parcelas já pagas (opcional)
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
                     WHERE obrigacao_id BETWEEN ? AND ?
                       AND categoria_evento='LANCAMENTO'
                    """,
                    (base_obrig_id, base_obrig_id + k - 1),
                )

            # log informativo de programação — inclui usuario e data_hora
            _ensure_mov_cols(cur)
            cur.execute(
                """
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao,
                     referencia_tabela, referencia_id, trans_uid, usuario, data_hora)
                VALUES (?, ?, 'info', 0, 'emprestimo_programado',
                        ?, 'contas_a_pagar_mov', ?, NULL, ?, ?)
                """,
                (
                    str(base_dt.date()),
                    credor,
                    f"Empréstimo programado {parcelas_total}x de R$ {valor_parcela:.2f} - "
                    f"{credor} (pagas na origem: {k})",
                    ids_lanc[0] if ids_lanc else None,
                    usuario,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )

            conn.commit()

        logger.debug(
            "programar_emprestimo: credor=%s parcelas=%s valor=%.2f base_obrig=%s pagas_iniciais=%s",
            credor, parcelas_total, valor_parcela, base_obrig_id, len(ids_pay)
        )
        return (ids_lanc, ids_pay)

    # ----------------------------------------------------------------------
    # Programar a partir do cadastro de empréstimos
    # ----------------------------------------------------------------------
    def programar_emprestimo_por_cadastro(
        self,
        *,
        cadastro: dict,
        usuario: str,
    ) -> tuple[list[int], list[int]]:
        """
        Cria programação de empréstimo a partir de um dict (cadastro) já existente.

        Espera chaves:
            - id (opcional), banco/descricao/tipo (um deles para credor),
            - data_primeira_parcela (YYYY-MM-DD),
            - parcelas_total (int), valor_parcela (float),
            - parcelas_ja_pagas (int, opcional).

        Retorna:
            (ids_lancamentos_cap, ids_pagamentos_iniciais)
        """
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
