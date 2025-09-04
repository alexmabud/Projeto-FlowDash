# flowdash_pages/lancamentos/saida/actions_pagamentos.py
"""
Actions de Pagamento (Fatura Cartão e Boleto)
---------------------------------------------
- Padrão ÚNICO aplicado aos dois fluxos:
  • MB (movimentações bancárias) registra o DINHEIRO que sai: (principal + juros + multa − desconto), mínimo 0
  • CAP recebe (via Ledger quando suportado; senão fallback local):
      principal_pago_acumulado += principal_cash
      desconto_aplicado_*      += desconto
      juros_pago_*             += juros
      multa_paga_*             += multa
      data_pagamento_*         =  data_str (coluna detectada dinamicamente)
  • Status QUITA quando principal_pago_acumulado + desconto_aplicado_* >= valor_evento.

Observação
----------
Estas actions não alteram o comportamento do Ledger; apenas chamam os registradores
com parâmetros corretos. Quando o Ledger não propaga tudo ao CAP, aplicamos um
fallback local idempotente diretamente na parcela/lançamento.
"""

from __future__ import annotations

from typing import Optional, Tuple
from shared.db import get_conn


# ---------------------------- Helpers ----------------------------

def _num(x) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def _mk_trans_uid(prefixo: str,
                  data_str: str,
                  identificador: str,
                  principal_cash: float,
                  juros: float,
                  multa: float,
                  desconto: float) -> str:
    # UID estável para idempotência
    return f"{prefixo}:{identificador}:{data_str}:{principal_cash:.2f}:{juros:.2f}:{multa:.2f}:{desconto:.2f}"


def _fmt_desc_pagamento(base: str,
                        principal_cash: float,
                        juros: float,
                        multa: float,
                        desconto: float) -> str:
    partes = [
        (base or "").strip() or "Pagamento",
        f"principal R${principal_cash:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
    ]
    if juros:
        partes.append(f"juros R${juros:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    if multa:
        partes.append(f"multa R${multa:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    if desconto:
        partes.append(f"desconto R${desconto:,.2f} (abatimento)".replace(",", "X").replace(".", ",").replace("X", "."))
    return " — ".join([p for p in partes if p])


def _pick_first_existing(cols_set: set[str], candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in cols_set:
            return c
    return None


# ---------------------------------------------------------------------
# PAGAMENTO DE FATURA (Cartão de Crédito)
# ---------------------------------------------------------------------
def pagar_fatura_action(*,
                        ledger,
                        data_str: str,
                        forma_pagamento: str,         # "DINHEIRO" ou bancária (PIX/DÉBITO etc.)
                        usuario_nome: str,
                        categoria: str,
                        subcat_nome: Optional[str],
                        descricao_base: str,
                        obrigacao_id_fatura: int,
                        # DINHEIRO:
                        origem_dinheiro: Optional[str] = None,
                        # BANCÁRIA:
                        caminho_banco: Optional[str] = None,      # usado no fallback de data_pagamento
                        banco_escolhido_in: Optional[str] = None,
                        # COMPONENTES:
                        principal_cash: float = 0.0,  # valor em dinheiro destinado ao principal
                        juros: float = 0.0,
                        multa: float = 0.0,
                        desconto: float = 0.0) -> Tuple[int, Optional[int]]:
    """
    Retorna (id_saida, id_mov_bancaria | None)
    """
    principal_cash = _num(principal_cash)
    juros          = _num(juros)
    multa          = _num(multa)
    desconto       = _num(desconto)

    if principal_cash < 0 or juros < 0 or multa < 0 or desconto < 0:
        raise ValueError("Valores negativos não são permitidos.")

    desc_final = _fmt_desc_pagamento(
        base=descricao_base or "Pagamento de fatura",
        principal_cash=principal_cash, juros=juros, multa=multa, desconto=desconto
    )

    uid = _mk_trans_uid(
        prefixo="PAG_FATURA",
        data_str=data_str,
        identificador=str(obrigacao_id_fatura),
        principal_cash=principal_cash, juros=juros, multa=multa, desconto=desconto
    )

    # Dinheiro que sai do caixa/banco: principal + juros + multa − desconto (mín. 0)
    valor_mb = principal_cash + juros + multa - desconto
    if valor_mb < 0:
        valor_mb = 0.0

    # ---------- Registrar saída ----------
    if forma_pagamento == "DINHEIRO":
        try:
            id_saida, id_mov = ledger.registrar_saida_dinheiro(
                data=data_str,
                valor=float(principal_cash),
                origem_dinheiro=origem_dinheiro,
                categoria=categoria,
                sub_categoria=subcat_nome,
                descricao=desc_final,
                usuario=usuario_nome,
                obrigacao_id_fatura=int(obrigacao_id_fatura),
                juros=float(juros),
                multa=float(multa),
                desconto=float(desconto),
                trans_uid=uid,
            )
        except TypeError:
            id_saida, id_mov = ledger.registrar_saida_dinheiro(
                data=data_str,
                valor=float(principal_cash),
                origem_dinheiro=origem_dinheiro,
                categoria=categoria,
                sub_categoria=subcat_nome,
                descricao=desc_final,
                usuario=usuario_nome,
                obrigacao_id_fatura=int(obrigacao_id_fatura),
                juros=float(juros),
                multa=float(multa),
                desconto=float(desconto),
            )
    else:
        try:
            from flowdash_pages.lancamentos.shared_ui import canonicalizar_banco
            banco_nome = canonicalizar_banco(caminho_banco or "", (banco_escolhido_in or "").strip()) or (banco_escolhido_in or "").strip()
        except Exception:
            banco_nome = (banco_escolhido_in or "").strip() or "Banco 1"

        try:
            id_saida, id_mov = ledger.registrar_saida_bancaria(
                data=data_str,
                valor=float(principal_cash),
                banco_nome=banco_nome,
                forma=forma_pagamento,
                categoria=categoria,
                sub_categoria=subcat_nome,
                descricao=desc_final,
                usuario=usuario_nome,
                obrigacao_id_fatura=int(obrigacao_id_fatura),
                juros=float(juros),
                multa=float(multa),
                desconto=float(desconto),
                trans_uid=uid,
            )
        except TypeError:
            id_saida, id_mov = ledger.registrar_saida_bancaria(
                data=data_str,
                valor=float(principal_cash),
                banco_nome=banco_nome,
                forma=forma_pagamento,
                categoria=categoria,
                sub_categoria=subcat_nome,
                descricao=desc_final,
                usuario=usuario_nome,
                obrigacao_id_fatura=int(obrigacao_id_fatura),
                juros=float(juros),
                multa=float(multa),
                desconto=float(desconto),
            )

    # ---------- Ajuste opcional do valor da MB (força sair = principal+juros+multa−desconto) ----------
    try:
        if hasattr(ledger, "ajustar_valor_movimentacao_por_uid"):
            ledger.ajustar_valor_movimentacao_por_uid(trans_uid=uid, novo_valor=float(valor_mb))
    except Exception:
        pass

    # ---------- Fallback: marcar data de pagamento no CAP da fatura ----------
    try:
        if caminho_banco:
            with get_conn(caminho_banco) as conn:
                cur = conn.cursor()
                cols = {r[1] for r in cur.execute("PRAGMA table_info(contas_a_pagar_mov)").fetchall()}
                col_data_pgto = _pick_first_existing(cols, [
                    "data_pagamento", "data_pgto", "data_baixa", "data_ultimo_pagamento",
                    "data_ultima_baixa", "data_pago"
                ])
                if col_data_pgto:
                    cur.execute(
                        f"""
                        UPDATE contas_a_pagar_mov
                           SET {col_data_pgto} = ?
                         WHERE obrigacao_id = ?
                           AND categoria_evento = 'LANCAMENTO'
                        """,
                        (data_str, int(obrigacao_id_fatura)),
                    )
                    conn.commit()
    except Exception:
        pass

    return id_saida, id_mov


# ---------------------------------------------------------------------
# PAGAMENTO DE BOLETO (Parcela)
# ---------------------------------------------------------------------
def pagar_boleto_action(*,
                        ledger,
                        data_str: str,
                        forma_pagamento: str,         # "DINHEIRO" ou bancária
                        usuario_nome: str,
                        categoria: str,
                        subcat_nome: Optional[str],
                        descricao_base: str,
                        obrigacao_id_boleto: int,     # mantido por compat, NÃO enviado ao Ledger
                        parcela_id_boleto: Optional[int] = None,  # ESSENCIAL p/ CAP fallback
                        # DINHEIRO:
                        origem_dinheiro: Optional[str] = None,
                        # BANCÁRIA:
                        caminho_banco: Optional[str] = None,
                        banco_escolhido_in: Optional[str] = None,
                        # COMPONENTES:
                        principal_cash: float = 0.0,
                        juros: float = 0.0,
                        multa: float = 0.0,
                        desconto: float = 0.0) -> Tuple[int, Optional[int]]:
    """
    Retorna (id_saida, id_mov_bancaria | None)

    - Não envia obrigacao_id_boleto/parcela_id_boleto para o Ledger (API não suporta).
    - Após registrar a saída, aplica fallback idempotente no CAP da PARCELA (se informado),
      e marca data de pagamento.
    """
    principal_cash = _num(principal_cash)
    juros          = _num(juros)
    multa          = _num(multa)
    desconto       = _num(desconto)

    if principal_cash < 0 or juros < 0 or multa < 0 or desconto < 0:
        raise ValueError("Valores negativos não são permitidos.")

    desc_final = _fmt_desc_pagamento(
        base=descricao_base or "Pagamento de boleto",
        principal_cash=principal_cash, juros=juros, multa=multa, desconto=desconto
    )

    uid = _mk_trans_uid(
        prefixo="PAG_BOLETO",
        data_str=data_str,
        identificador=str(parcela_id_boleto or obrigacao_id_boleto),
        principal_cash=principal_cash, juros=juros, multa=multa, desconto=desconto
    )

    # Dinheiro que sai do caixa/banco: principal + juros + multa − desconto (mín. 0)
    valor_mb = principal_cash + juros + multa - desconto
    if valor_mb < 0:
        valor_mb = 0.0

    # ---------- 1) Registrar a saída no Ledger (sem kwargs não suportados) ----------
    if forma_pagamento == "DINHEIRO":
        try:
            id_saida, id_mov = ledger.registrar_saida_dinheiro(
                data=data_str,
                valor=float(principal_cash),
                origem_dinheiro=origem_dinheiro,
                categoria=categoria,
                sub_categoria=subcat_nome,
                descricao=desc_final,
                usuario=usuario_nome,
                juros=float(juros),
                multa=float(multa),
                desconto=float(desconto),
                trans_uid=uid,  # se o Ledger ignorar, não falha
            )
        except TypeError:
            id_saida, id_mov = ledger.registrar_saida_dinheiro(
                data=data_str,
                valor=float(principal_cash),
                origem_dinheiro=origem_dinheiro,
                categoria=categoria,
                sub_categoria=subcat_nome,
                descricao=desc_final,
                usuario=usuario_nome,
                juros=float(juros),
                multa=float(multa),
                desconto=float(desconto),
            )
    else:
        # Bancária
        try:
            from flowdash_pages.lancamentos.shared_ui import canonicalizar_banco
            banco_nome = canonicalizar_banco(caminho_banco or "", (banco_escolhido_in or "").strip()) or (banco_escolhido_in or "").strip()
        except Exception:
            banco_nome = (banco_escolhido_in or "").strip() or "Banco 1"

        try:
            id_saida, id_mov = ledger.registrar_saida_bancaria(
                data=data_str,
                valor=float(principal_cash),
                banco_nome=banco_nome,
                forma=forma_pagamento,
                categoria=categoria,
                sub_categoria=subcat_nome,
                descricao=desc_final,
                usuario=usuario_nome,
                juros=float(juros),
                multa=float(multa),
                desconto=float(desconto),
                trans_uid=uid,
            )
        except TypeError:
            id_saida, id_mov = ledger.registrar_saida_bancaria(
                data=data_str,
                valor=float(principal_cash),
                banco_nome=banco_nome,
                forma=forma_pagamento,
                categoria=categoria,
                sub_categoria=subcat_nome,
                descricao=desc_final,
                usuario=usuario_nome,
                juros=float(juros),
                multa=float(multa),
                desconto=float(desconto),
            )

    # ---------- Ajuste opcional do valor da MB (força sair = principal+juros+multa−desconto) ----------
    try:
        if hasattr(ledger, "ajustar_valor_movimentacao_por_uid"):
            ledger.ajustar_valor_movimentacao_por_uid(trans_uid=uid, novo_valor=float(valor_mb))
    except Exception:
        pass

    # ---------- 2) Fallback CAP idempotente na PARCELA + data de pagamento ----------
    try:
        if caminho_banco and parcela_id_boleto:
            with get_conn(caminho_banco) as conn:
                cur = conn.cursor()

                # Quais colunas existem?
                cols = {r[1] for r in cur.execute("PRAGMA table_info(contas_a_pagar_mov)").fetchall()}

                # Selecionar dinamicamente colunas para leitura
                # valor_evento
                if "valor_evento" in cols:
                    col_evento = "valor_evento"
                elif "valor" in cols:
                    col_evento = "valor"
                elif "valor_total" in cols:
                    col_evento = "valor_total"
                elif "valor_original" in cols:
                    col_evento = "valor_original"
                else:
                    col_evento = None  # literal 0.0

                # principal pago acumulado (ou total pago)
                if "principal_pago_acumulado" in cols:
                    col_princ = "principal_pago_acumulado"
                elif "valor_pago_acumulado" in cols:
                    col_princ = "valor_pago_acumulado"
                else:
                    col_princ = None

                # desconto acumulado
                if "desconto_aplicado_acumulado" in cols:
                    col_desc = "desconto_aplicado_acumulado"
                elif "desconto_aplicado" in cols:
                    col_desc = "desconto_aplicado"
                elif "desconto" in cols:
                    col_desc = "desconto"
                else:
                    col_desc = None

                # juros/multa acumulados
                col_juros = "juros_pago_acumulado" if "juros_pago_acumulado" in cols else None
                col_multa = "multa_paga_acumulada" if "multa_paga_acumulada" in cols else None

                # coluna de data de pagamento
                col_data_pgto = _pick_first_existing(cols, [
                    "data_pagamento", "data_pgto", "data_baixa", "data_ultimo_pagamento",
                    "data_ultima_baixa", "data_pago"
                ])

                select_list = []
                select_list.append(f"COALESCE({col_evento}, 0.0) AS valor_evento" if col_evento else "0.0 AS valor_evento")
                select_list.append(f"COALESCE({col_princ}, 0.0) AS principal_pago_acum" if col_princ else "0.0 AS principal_pago_acum")
                select_list.append(f"COALESCE({col_desc}, 0.0) AS desconto_acum" if col_desc else "0.0 AS desconto_acum")
                select_list.append(f"COALESCE({col_juros}, 0.0) AS juros_acum" if col_juros else "0.0 AS juros_acum")
                select_list.append(f"COALESCE({col_multa}, 0.0) AS multa_acum" if col_multa else "0.0 AS multa_acum")

                sql_sel = f"""
                    SELECT {", ".join(select_list)}
                      FROM contas_a_pagar_mov
                     WHERE id = ?
                       AND categoria_evento = 'LANCAMENTO'
                     LIMIT 1
                """

                row = cur.execute(sql_sel, (int(parcela_id_boleto),)).fetchone()

                if row:
                    valor_evento, princ_acum, desc_acum, juros_acum, multa_acum = map(float, row)

                    # Quanto de principal ainda cabe (após o que já foi aplicado)
                    saldo_principal = max(valor_evento - princ_acum - desc_acum, 0.0)
                    inc_principal   = max(0.0, min(principal_cash, saldo_principal))

                    # Desconto também abate principal; limitar ao saldo remanescente pós-inc_principal
                    saldo_pos_princ = max(valor_evento - (princ_acum + inc_principal) - desc_acum, 0.0)
                    inc_desconto    = max(0.0, min(desconto, saldo_pos_princ))

                    # Juros/multa acumulam
                    inc_juros = float(juros) if abs(float(juros)) > 0.0005 else 0.0
                    inc_multa = float(multa) if abs(float(multa)) > 0.0005 else 0.0

                    set_parts, params = [], []

                    if "principal_pago_acumulado" in cols and inc_principal > 0.0005:
                        set_parts.append("principal_pago_acumulado = COALESCE(principal_pago_acumulado,0) + ?")
                        params.append(inc_principal)

                    if inc_desconto > 0.0005:
                        if "desconto_aplicado_acumulado" in cols:
                            set_parts.append("desconto_aplicado_acumulado = COALESCE(desconto_aplicado_acumulado,0) + ?")
                        elif "desconto_aplicado" in cols:
                            set_parts.append("desconto_aplicado = COALESCE(desconto_aplicado,0) + ?")
                        elif "desconto" in cols:
                            set_parts.append("desconto = COALESCE(desconto,0) + ?")
                        else:
                            inc_desconto = 0.0
                        if inc_desconto > 0.0:
                            params.append(inc_desconto)

                    if "juros_pago_acumulado" in cols and inc_juros > 0.0005:
                        set_parts.append("juros_pago_acumulado = COALESCE(juros_pago_acumulado,0) + ?")
                        params.append(inc_juros)

                    if "multa_paga_acumulada" in cols and inc_multa > 0.0005:
                        set_parts.append("multa_paga_acumulada = COALESCE(multa_paga_acumulada,0) + ?")
                        params.append(inc_multa)

                    # Compat legado: total pago em dinheiro (sem desconto)
                    soma_total_inc = inc_principal + inc_juros + inc_multa
                    if "valor_pago_acumulado" in cols and soma_total_inc > 0.0005:
                        set_parts.append("valor_pago_acumulado = COALESCE(valor_pago_acumulado,0) + ?")
                        params.append(soma_total_inc)

                    # Status
                    novo_princ = princ_acum + inc_principal
                    novo_desc  = desc_acum + inc_desconto
                    quitada = (novo_princ + novo_desc) >= (valor_evento - 0.005)
                    if "status" in cols:
                        set_parts.append("status = ?")
                        params.append("QUITADA" if quitada else "PARCIAL")

                    # Data de pagamento
                    if col_data_pgto:
                        set_parts.append(f"{col_data_pgto} = ?")
                        params.append(data_str)

                    if set_parts:
                        sql_upd = f"""
                            UPDATE contas_a_pagar_mov
                               SET {", ".join(set_parts)}
                             WHERE id = ?
                               AND categoria_evento = 'LANCAMENTO'
                        """
                        params.append(int(parcela_id_boleto))
                        cur.execute(sql_upd, params)
                        conn.commit()
    except Exception:
        # Falha no fallback CAP não deve travar o pagamento
        pass

    return id_saida, id_mov
