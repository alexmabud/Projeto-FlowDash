
"""Empréstimos (pagamento e programação) (_EmprestimoLedgerMixin)."""
from __future__ import annotations
from typing import Optional, Tuple, List
import pandas as pd
from shared.db import get_conn
from shared.ids import sanitize

class _EmprestimoLedgerMixin:
    """Mixin com regras para empréstimos (pagar parcela e programar)."""

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
        v_pg = max(0.0, float(valor))
        v_multa = max(0.0, float(multa or 0.0))
        v_juros = max(0.0, float(juros or 0.0))
        v_desc  = max(0.0, float(desconto or 0.0))

        cat  = sanitize(categoria)
        sub  = sanitize(sub_categoria)
        desc = sanitize(descricao)
        usu  = sanitize(usuario)
        org  = sanitize(origem)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            saldo_atual = self.cap_repo.obter_saldo_obrigacao(conn, int(obrigacao_id))
            eps = 0.005
            valor_a_pagar = min(v_pg, max(saldo_atual, 0.0))
            total_saida = max(valor_a_pagar + v_multa + v_juros - v_desc, 0.0)

            if forma_pagamento == "DINHEIRO":
                self._garantir_linha_saldos_caixas(conn, data)
                cur.execute("""
                    INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                       Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                       Origem_Dinheiro, Banco_Saida)
                    VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
                """, (data, cat, sub, desc, float(total_saida), usu, org))
                id_saida = int(cur.lastrowid)

                campo = "caixa" if org == "Caixa" else "caixa_2"
                cur.execute(f"""
                    UPDATE saldos_caixas SET {campo} = COALESCE({campo},0) - ?
                    WHERE data = ?
                """, (float(total_saida), data))

                obs = (f"Pagamento Empréstimo {cat}/{sub or ''}".strip()
                    + (f" - {desc}" if desc else "")
                    + (f" | multa R$ {v_multa:.2f}" if v_multa>0 else "")
                    + (f", juros R$ {v_juros:.2f}" if v_juros>0 else "")
                    + (f", desconto R$ {v_desc:.2f}" if v_desc>0 else ""))
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                    VALUES (?, ?, 'saida', ?, 'saidas_emprestimo_pagamento', ?, 'saida', ?, ?)
                """, (data, org, float(total_saida), obs, id_saida, trans_uid))
                id_mov = int(cur.lastrowid)

            else:
                self._garantir_linha_saldos_bancos(conn, data)
                self._ajustar_banco_dynamic(conn, banco_col=org, delta=-float(total_saida), data=data)

                cur.execute("""
                    INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                       Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                       Origem_Dinheiro, Banco_Saida)
                    VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
                """, (data, cat, sub, desc, forma_pagamento, float(total_saida), usu, org))
                id_saida = int(cur.lastrowid)

                obs = (f"Pagamento Empréstimo {cat}/{sub or ''}".strip()
                    + (f" - {desc}" if desc else "")
                    + (f" | multa R$ {v_multa:.2f}" if v_multa>0 else "")
                    + (f", juros R$ {v_juros:.2f}" if v_juros>0 else "")
                    + (f", desconto R$ {v_desc:.2f}" if v_desc>0 else ""))
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                    VALUES (?, ?, 'saida', ?, 'saidas_emprestimo_pagamento', ?, 'saida', ?, ?)
                """, (data, org, float(total_saida), obs, id_saida, trans_uid))
                id_mov = int(cur.lastrowid)

            evento_id = self.cap_repo.registrar_pagamento(
                conn,
                obrigacao_id=int(obrigacao_id),
                tipo_obrigacao="EMPRESTIMO",
                valor_pago=float(total_saida),
                data_evento=data,
                forma_pagamento=forma_pagamento,
                origem=org,
                ledger_id=int(id_saida),
                usuario=usu
            )

            try:
                row = conn.execute("""
                    SELECT id, COALESCE(valor_evento,0) AS valor_parcela
                    FROM contas_a_pagar_mov
                    WHERE obrigacao_id = ?
                      AND categoria_evento = 'LANCAMENTO'
                    LIMIT 1
                """, (int(obrigacao_id),)).fetchone()
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
            except Exception:
                pass

            conn.commit()
            return (id_saida, id_mov, int(evento_id))

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
        parcelas_ja_pagas: int = 0
    ) -> tuple[list[int], list[int]]:
        credor = sanitize(credor or "").strip() or "Empréstimo"
        usuario = sanitize(usuario or "Sistema")
        desc   = sanitize(descricao)

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
        ids_pay:  list[int] = []

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            base_obrig_id = self.cap_repo.proximo_obrigacao_id(conn)

            for i in range(1, int(parcelas_total) + 1):
                vcto = (base_dt + pd.DateOffset(months=i-1)).date()
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
                    usuario=usuario
                )
                ids_lanc.append(int(lanc_id))

            if emprestimo_id is not None:
                try:
                    emprestimo_id = int(emprestimo_id)
                except Exception:
                    emprestimo_id = None

            if emprestimo_id is not None:
                cur.execute("""
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='EMPRESTIMO',
                           emprestimo_id=?,
                           cartao_id=NULL,
                           status = COALESCE(NULLIF(status,''), 'Em aberto')
                     WHERE obrigacao_id BETWEEN ? AND ?
                """, (int(emprestimo_id), base_obrig_id, base_obrig_id + int(parcelas_total) - 1))
            else:
                cur.execute("""
                    UPDATE contas_a_pagar_mov
                       SET tipo_origem='EMPRESTIMO',
                           cartao_id=NULL,
                           status = COALESCE(NULLIF(status,''), 'Em aberto')
                     WHERE obrigacao_id BETWEEN ? AND ?
                """, (base_obrig_id, base_obrig_id + int(parcelas_total) - 1))

            k = max(0, min(int(parcelas_ja_pagas or 0), int(parcelas_total)))
            if k > 0:
                for i in range(0, k):
                    obrig_id = base_obrig_id + i
                    row = cur.execute("""
                        SELECT COALESCE(vencimento, data_evento) AS vcto
                          FROM contas_a_pagar_mov
                         WHERE obrigacao_id = ?
                           AND categoria_evento='LANCAMENTO'
                         LIMIT 1
                    """, (obrig_id,)).fetchone()
                    vcto = (row[0] if row and row[0] else str((base_dt + pd.DateOffset(months=i)).date()))

                    ev_id = self.cap_repo.registrar_pagamento(
                        conn,
                        obrigacao_id=int(obrig_id),
                        tipo_obrigacao="EMPRESTIMO",
                        valor_pago=float(valor_parcela),
                        data_evento=str(vcto),
                        forma_pagamento="AJUSTE",
                        origem="programacao",
                        ledger_id=None,
                        usuario=usuario
                    )
                    ids_pay.append(int(ev_id))

                cur.execute("""
                    UPDATE contas_a_pagar_mov
                       SET status='Quitado'
                     WHERE obrigacao_id BETWEEN ? AND ?
                       AND categoria_evento='LANCAMENTO'
                """, (base_obrig_id, base_obrig_id + k - 1))

            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'info', 0, 'emprestimo_programado',
                        ?, 'contas_a_pagar_mov', ?, NULL)
            """, (
                str(base_dt.date()),
                credor,
                f"Empréstimo programado {parcelas_total}x de R$ {valor_parcela:.2f} - "
                f"{credor} (pagas na origem: {k})",
                ids_lanc[0] if ids_lanc else None
            ))

            conn.commit()

        return (ids_lanc, ids_pay)

    def programar_emprestimo_por_cadastro(
        self,
        *,
        cadastro: dict,
        usuario: str
    ) -> tuple[list[int], list[int]]:
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
            import pandas as pd
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
            parcelas_ja_pagas=pp
        )
