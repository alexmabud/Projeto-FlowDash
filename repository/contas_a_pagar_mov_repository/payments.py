# -*- coding: utf-8 -*-
"""
Módulo Pagamentos (Contas a Pagar - Mixins)
===========================================

Define a classe `PaymentsMixin`, responsável por validações e registro de
eventos de **pagamento** (boletos/faturas/empréstimos) e por atualizar
acumulados na tabela `contas_a_pagar_mov`.

Regras de acumulação (PARCIAL e QUITAÇÃO TOTAL)
-----------------------------------------------
- `valor_pago_acumulado` **acumula**: principal_aplicado + juros + multa.
  (⚠️ Não subtrai `desconto`.)
- `juros_pago`, `multa_paga`/`multa_pago` e `desconto_aplicado`/`desconto`
  também são acumulados nas suas colunas.
- O **status** e o **restante** são calculados pela base estendida:
    restante = valor_evento + juros_pago + multa_pago - desconto_aplicado - valor_pago_acumulado
  * `QUITADO` quando `restante <= 0,005`
  * `Parcial` quando `restante > 0,005` e `valor_pago_acumulado > 0`
  * `Em aberto` quando `valor_pago_acumulado == 0`
- `data_pagamento` (se existir no schema) é preenchida **somente** quando
  virar `QUITADO` (usa a data do evento ou a atual).

Eventos CAP (registro financeiro)
---------------------------------
- O desembolso financeiro enviado ao logger/evento é:
    saida_total = max(0, principal_aplicado + juros + multa - desconto)
- O evento CAP de pagamento é registrado com **valor negativo** (BaseRepo).

Compatibilidade de schema
-------------------------
- Aceita colunas `multa_pago` **OU** `multa_paga`, e `desconto` **OU**
  `desconto_aplicado`.
- Se `valor_pago_acumulado` não existir, o código ignora sua atualização.

"""

from __future__ import annotations

from typing import Optional, Any, Dict
from decimal import Decimal
from datetime import date as _date

from repository.contas_a_pagar_mov_repository.types import _q2


class PaymentsMixin(object):
    """Operações de pagamento (boletos/faturas/emprestimos)."""

    def __init__(self, *args, **kwargs) -> None:
        # __init__ cooperativo para múltipla herança
        super().__init__(*args, **kwargs)

    # ---------------------------------------------------------------------
    # Helpers internos
    # ---------------------------------------------------------------------
    def _conn_ctx(self, conn: Any):
        """
        Context manager de conexão:
        - Se `conn` for fornecido (sqlite3.Connection), usa-o diretamente.
        - Se `conn` for None, abre via `self._get_conn()` (fornecido por BaseRepo).
        """
        if conn is not None:
            class _DummyCtx:
                def __init__(self, c): self.c = c
                def __enter__(self): return self.c
                def __exit__(self, exc_type, exc, tb): return False
            return _DummyCtx(conn)
        return self._get_conn()  # type: ignore[attr-defined]

    def _schema_cols(self, cur) -> set[str]:
        cols = []
        try:
            cols = [r[1] for r in cur.execute("PRAGMA table_info(contas_a_pagar_mov)").fetchall()]
        except Exception:
            pass
        return set(cols)

    def _get_row(self, cur, parcela_id: int):
        return cur.execute(
            "SELECT * FROM contas_a_pagar_mov WHERE id = ? LIMIT 1",
            (int(parcela_id),),
        ).fetchone()

    # ---------------------------------------------------------------------
    # Regras/validações
    # ---------------------------------------------------------------------
    def _validar_pagamento_nao_excede_saldo(
        self,
        conn: Any = None,
        obrigacao_id: int = 0,
        valor_pago: float = 0.0,
    ) -> float:
        """
        Garante que o pagamento não exceda o saldo em aberto (vw_cap_saldos).

        Retorna:
            float: saldo atual em aberto.

        Lança:
            ValueError: se pagamento exceder (tolerância 0,005) ou valor_pago <= 0.
        """
        valor_pago = float(valor_pago)
        if valor_pago <= 0:
            raise ValueError("O valor do pagamento deve ser positivo.")

        with self._conn_ctx(conn) as c:
            row = c.execute(
                "SELECT COALESCE(saldo_aberto,0) FROM vw_cap_saldos WHERE obrigacao_id=?;",
                (int(obrigacao_id),),
            ).fetchone()
            saldo = float(row[0]) if row else 0.0

        eps = 0.005
        if valor_pago > saldo + eps:
            raise ValueError(f"Pagamento (R$ {valor_pago:.2f}) maior que o saldo (R$ {saldo:.2f}).")
        return saldo

    # ---------------------------------------------------------------------
    # Eventos de pagamento (boleto simples/legado)
    # ---------------------------------------------------------------------
    def registrar_pagamento_parcela_boleto(
        self,
        conn: Any = None,
        *,
        obrigacao_id: int,
        valor_pago: float,
        data_evento: str,          # 'YYYY-MM-DD'
        forma_pagamento: str,
        origem: str,               # 'Caixa' / 'Caixa 2' / nome do banco
        ledger_id: int,
        usuario: str,
        descricao_extra: Optional[str] = None,
    ) -> int:
        """
        Insere um evento **PAGAMENTO** (valor_evento negativo) para um boleto.
        """
        # 1) valida saldo
        self._validar_pagamento_nao_excede_saldo(conn, int(obrigacao_id), float(valor_pago))

        # 2) validações básicas do evento
        self._validar_evento_basico(
            obrigacao_id=int(obrigacao_id),
            tipo_obrigacao="BOLETO",
            categoria_evento="PAGAMENTO",
            data_evento=data_evento,
            valor_evento=-abs(float(valor_pago)),
            usuario=usuario,
        )

        # 3) insere evento (PAGAMENTO é negativo)
        with self._conn_ctx(conn) as c:
            return self._inserir_evento(  # fornecido por BaseRepo
                c,
                obrigacao_id=int(obrigacao_id),
                tipo_obrigacao="BOLETO",
                categoria_evento="PAGAMENTO",
                data_evento=data_evento,
                vencimento=None,
                valor_evento=-abs(float(valor_pago)),
                descricao=descricao_extra,
                credor=None,
                competencia=None,
                parcela_num=None,
                parcelas_total=None,
                forma_pagamento=forma_pagamento,
                origem=origem,
                ledger_id=int(ledger_id) if ledger_id is not None else None,
                usuario=usuario,
            )

    # ---------------------------------------------------------------------
    # Quitação TOTAL (acumula principal + juros + multa)
    # ---------------------------------------------------------------------
    def aplicar_pagamento_parcela_quitacao_total(
        self,
        conn: Any = None,
        *,
        parcela_id: int,
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
        data_evento: str,          # 'YYYY-MM-DD'
        forma_pagamento: str,      # "DINHEIRO" | "PIX" | "DÉBITO" | etc
        origem: str,               # "Caixa" | "Caixa 2" | nome do banco/cartão
        ledger_id: int,            # id da saída/mov bancária
        usuario: str,
        trans_uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Quitação total:
          - principal_aplicado = restante do principal
          - valor_pago_acumulado += (principal_aplicado + juros + multa)
          - acumula juros/multa/desconto nas colunas próprias
          - status = 'QUITADO' e preenche data_pagamento
          - registra evento CAP (negativo) com o desembolso: principal+juros+multa-desconto
        """
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            cols = self._schema_cols(cur)
            row = self._get_row(cur, int(parcela_id))
            if not row:
                raise ValueError(f"Parcela id={parcela_id} não encontrada em contas_a_pagar_mov")

            obrigacao_id = int(row["obrigacao_id"]) if "obrigacao_id" in row.keys() else 0
            tipo_obrig   = str(row["tipo_obrigacao"]) if "tipo_obrigacao" in row.keys() else ""
            valor_evento = float(row["valor_evento"]) if "valor_evento" in row.keys() else 0.0

            vpa_exists      = "valor_pago_acumulado" in cols
            vpa_atual       = float(row["valor_pago_acumulado"]) if vpa_exists else 0.0
            juros_prev      = float(row["juros_pago"]) if "juros_pago" in row.keys() else 0.0

            # multa: multa_pago ou multa_paga
            if "multa_pago" in row.keys():
                multa_prev = float(row["multa_pago"]); multa_col = "multa_pago"
            elif "multa_paga" in row.keys():
                multa_prev = float(row["multa_paga"]); multa_col = "multa_paga"
            else:
                multa_prev = 0.0; multa_col = "multa_paga"

            # desconto: desconto ou desconto_aplicado
            if "desconto" in row.keys():
                desc_prev = float(row["desconto"]); desc_col = "desconto"
            elif "desconto_aplicado" in row.keys():
                desc_prev = float(row["desconto_aplicado"]); desc_col = "desconto_aplicado"
            else:
                desc_prev = 0.0; desc_col = "desconto_aplicado"

            data_pag_exists = "data_pagamento" in cols
            usuario_exists  = "usuario" in cols

            juros    = float(juros or 0.0)
            multa    = float(multa or 0.0)
            desconto = float(desconto or 0.0)

            # principal já pago até aqui = vpa_atual - juros_prev - multa_prev
            principal_pago_ate_agora = max(0.0, vpa_atual - juros_prev - multa_prev)
            restante_principal = max(0.0, valor_evento - principal_pago_ate_agora)
            principal_aplicado = restante_principal  # quitação: zera o principal

            # desembolso financeiro
            saida_total = principal_aplicado + juros + multa - desconto
            if saida_total < 0:
                saida_total = 0.0

            # novos acumulados
            novo_vpa   = _q2(Decimal(str(vpa_atual)) + Decimal(str(principal_aplicado)) + Decimal(str(juros)) + Decimal(str(multa)))
            novo_juros = _q2(Decimal(str(juros_prev)) + Decimal(str(juros)))
            novo_multa = _q2(Decimal(str(multa_prev)) + Decimal(str(multa)))
            novo_desc  = _q2(Decimal(str(desc_prev)) + Decimal(str(desconto)))

            # status/saldo pela base estendida
            saldo_ext = _q2(
                Decimal(str(valor_evento)) + Decimal(str(novo_juros)) + Decimal(str(novo_multa))
                - Decimal(str(novo_desc)) - Decimal(str(novo_vpa))
            )
            eps = Decimal("0.005")
            status_final = "QUITADO" if saldo_ext <= eps else "Parcial"
            data_para_gravar = (data_evento or _date.today().isoformat()) if status_final == "QUITADO" else None

            # UPDATE
            set_parts = [
                "juros_pago = ?",
                f"{multa_col} = ?",
                f"{desc_col} = ?",
                "status = ?",
            ]
            params = [float(novo_juros), float(novo_multa), float(novo_desc), status_final]

            if vpa_exists:
                set_parts.insert(0, "valor_pago_acumulado = ?")
                params.insert(0, float(novo_vpa))

            if data_pag_exists and data_para_gravar:
                set_parts.append("data_pagamento = ?")
                params.append(str(data_para_gravar))

            if usuario_exists and usuario:
                set_parts.append("usuario = ?")
                params.append(str(usuario))

            sql = f"UPDATE contas_a_pagar_mov SET {', '.join(set_parts)} WHERE id = ?"
            params.append(int(parcela_id))
            cur.execute(sql, params)

            # evento CAP de pagamento (negativo na BaseRepo)
            id_evento_cap = self.registrar_pagamento(
                c,
                obrigacao_id=obrigacao_id,
                tipo_obrigacao=tipo_obrig,
                valor_pago=float(saida_total),
                data_evento=str(data_evento),
                forma_pagamento=str(forma_pagamento),
                origem=str(origem),
                ledger_id=int(ledger_id),
                usuario=str(usuario),
            )

            # padroniza LANCAMENTO(s) da obrigação se quitou
            if status_final == "QUITADO":
                cur.execute(
                    """
                    UPDATE contas_a_pagar_mov
                       SET status='QUITADO'
                     WHERE obrigacao_id = ?
                       AND categoria_evento='LANCAMENTO'
                    """,
                    (int(obrigacao_id),),
                )

            c.commit()

            return {
                "parcela_id": int(parcela_id),
                "obrigacao_id": int(obrigacao_id),
                "tipo_obrigacao": tipo_obrig,
                "saida_total": float(saida_total),
                "id_evento_cap": int(id_evento_cap),
                "status": status_final,
                "valor_evento": float(valor_evento),
                "pago_acumulado": float(novo_vpa),
                "restante": float(saldo_ext) if float(saldo_ext) > 0 else 0.0,
            }

    # ---------------------------------------------------------------------
    # Pagamento PARCIAL/TOTAL (acumula principal + juros + multa)
    # ---------------------------------------------------------------------
    def aplicar_pagamento_parcela(self, conn: Any = None, payload: Optional[dict] = None, *args, **kwargs) -> dict:
        """
        Aplica pagamento PARCIAL/TOTAL:
          - principal_aplicado = clamp(pedido, restante_principal)
          - valor_pago_acumulado += (principal_aplicado + juros + multa)
          - acumula juros/multa/desconto
          - status pela base estendida (ver docstring)
          - data_pagamento preenchida apenas quando QUITADO
        Aceita tanto 'valor_base' (preferido) quanto 'valor_pago' (legado).
        """

        def _f(x, default=0.0) -> float:
            try:
                return float(x if x is not None else default)
            except Exception:
                return float(default)

        # -------- normalização de entrada (payload/args/kwargs) --------
        if isinstance(payload, dict):
            parcela_id    = int(payload.get("parcela_id") or payload.get("evento_id") or payload.get("id"))
            principal_inc = _f(payload.get("valor_base", payload.get("valor_pago", payload.get("valor_pagamento", 0.0))))
            juros_inc     = _f(payload.get("juros"))
            multa_inc     = _f(payload.get("multa"))
            desconto_inc  = _f(payload.get("desconto"))
            data_pgto     = payload.get("data_pagamento") or payload.get("data_evento")
        elif args:
            parcela_id    = int(args[0])
            principal_inc = _f(args[1] if len(args) > 1 else 0.0)
            juros_inc     = _f(args[2] if len(args) > 2 else 0.0)
            multa_inc     = _f(args[3] if len(args) > 3 else 0.0)
            desconto_inc  = _f(args[4] if len(args) > 4 else 0.0)
            data_pgto     = args[5] if len(args) > 5 else (kwargs.get("data_pagamento") or kwargs.get("data_evento"))
        else:
            parcela_id    = int(kwargs.get("parcela_id") or kwargs.get("evento_id") or kwargs.get("id"))
            principal_inc = _f(kwargs.get("valor_base", kwargs.get("valor_pago", kwargs.get("valor_pagamento", 0.0))))
            juros_inc     = _f(kwargs.get("juros", kwargs.get("juros_incremento", 0.0)))
            multa_inc     = _f(kwargs.get("multa", kwargs.get("multa_incremento", 0.0)))
            desconto_inc  = _f(kwargs.get("desconto", kwargs.get("desconto_incremento", 0.0)))
            data_pgto     = kwargs.get("data_pagamento") or kwargs.get("data_evento")

        # Sanitiza
        principal_inc = max(0.0, principal_inc)
        juros_inc     = max(0.0, juros_inc)
        multa_inc     = max(0.0, multa_inc)
        desconto_inc  = max(0.0, desconto_inc)

        # ------------------------------ aplicação ------------------------------
        with self._conn_ctx(conn) as c:
            cur = c.cursor()
            cols = self._schema_cols(cur)
            row = self._get_row(cur, int(parcela_id))
            if not row:
                raise ValueError(f"Parcela id={parcela_id} não encontrada em contas_a_pagar_mov")

            valor_evento = float(row["valor_evento"]) if "valor_evento" in row.keys() else 0.0
            vpa_atual    = float(row["valor_pago_acumulado"]) if "valor_pago_acumulado" in row.keys() else 0.0
            juros_prev   = float(row["juros_pago"]) if "juros_pago" in row.keys() else 0.0

            # multa: aceita 'multa_pago' ou 'multa_paga'
            if "multa_pago" in row.keys():
                multa_prev = float(row["multa_pago"]); multa_col = "multa_pago"
            elif "multa_paga" in row.keys():
                multa_prev = float(row["multa_paga"]); multa_col = "multa_paga"
            else:
                multa_prev = 0.0; multa_col = "multa_paga"

            # desconto: aceita 'desconto' ou 'desconto_aplicado'
            if "desconto" in row.keys():
                desc_prev = float(row["desconto"]); desc_col = "desconto"
            elif "desconto_aplicado" in row.keys():
                desc_prev = float(row["desconto_aplicado"]); desc_col = "desconto_aplicado"
            else:
                desc_prev = 0.0; desc_col = "desconto_aplicado"

            # ----- clamp pelo RESTANTE do principal -----
            principal_pago_ate_agora = max(0.0, vpa_atual - juros_prev - multa_prev)
            restante_principal = max(0.0, valor_evento - principal_pago_ate_agora)
            principal_aplicado = min(principal_inc, restante_principal)

            # ----- novos acumulados -----
            novo_vpa   = _q2(Decimal(str(vpa_atual)) + Decimal(str(principal_aplicado)) + Decimal(str(juros_inc)) + Decimal(str(multa_inc)))
            novo_juros = _q2(Decimal(str(juros_prev)) + Decimal(str(juros_inc)))
            novo_multa = _q2(Decimal(str(multa_prev)) + Decimal(str(multa_inc)))
            novo_desc  = _q2(Decimal(str(desc_prev)) + Decimal(str(desconto_inc)))

            # ----- status/saldo pela base estendida -----
            saldo_ext = _q2(
                Decimal(str(valor_evento)) + Decimal(str(novo_juros)) + Decimal(str(novo_multa))
                - Decimal(str(novo_desc)) - Decimal(str(novo_vpa))
            )
            eps = Decimal("0.005")
            if saldo_ext <= eps:
                status_final = "QUITADO"
                data_para_gravar = data_pgto or _date.today().isoformat()
            elif novo_vpa > 0:
                status_final = "Parcial"
                data_para_gravar = None
            else:
                status_final = "Em aberto"
                data_para_gravar = None

            # ----- UPDATE dinâmico -----
            set_parts = [
                "juros_pago = ?",
                f"{multa_col} = ?",
                f"{desc_col} = ?",
                "status = ?",
            ]
            params = [float(novo_juros), float(novo_multa), float(novo_desc), status_final]

            if "valor_pago_acumulado" in cols:
                set_parts.insert(0, "valor_pago_acumulado = ?")
                params.insert(0, float(novo_vpa))

            if "data_pagamento" in cols and data_para_gravar:
                set_parts.append("data_pagamento = ?")
                params.append(str(data_para_gravar))

            sql = f"UPDATE contas_a_pagar_mov SET {', '.join(set_parts)} WHERE id = ?"
            params.append(int(parcela_id))
            cur.execute(sql, params)

            # se quitou, padroniza LANCAMENTOs da obrigação
            if status_final == "QUITADO" and "obrigacao_id" in row.keys():
                cur.execute(
                    """
                    UPDATE contas_a_pagar_mov
                       SET status='QUITADO'
                     WHERE obrigacao_id = ?
                       AND categoria_evento='LANCAMENTO'
                    """,
                    (int(row["obrigacao_id"]),),
                )

            c.commit()

        restante_depois = float(saldo_ext) if float(saldo_ext) > 0 else 0.0

        return {
            "parcela_id": int(parcela_id),
            "valor_parcela": float(_q2(Decimal(str(valor_evento)))),
            "pago_acumulado": float(novo_vpa),
            "status": status_final,
            "restante": float(_q2(Decimal(str(restante_depois)))),
            # compat com chamadores que esperavam essa chave
            "id_evento_cap": None,
        }


# API pública explícita
__all__ = ["PaymentsMixin"]
