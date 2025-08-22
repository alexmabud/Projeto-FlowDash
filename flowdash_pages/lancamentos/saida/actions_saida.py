# ===================== Actions: Saída =====================
"""
Executa a MESMA lógica do módulo original de Saída (sem Streamlit aqui):
- Fluxos padrão: DINHEIRO, PIX/DÉBITO, CRÉDITO, BOLETO.
- Fluxos Pagamentos: Fatura Cartão, Boletos (parcela), Empréstimos (parcela).
- Canonicalização de banco preservada.

Validações que no original exibiam st.warning/st.error aqui geram ValueError/RuntimeError.
A página captura e exibe as mensagens.
"""

from __future__ import annotations

from typing import TypedDict, Optional, List
from datetime import date

import pandas as pd
from shared.db import get_conn
from services.ledger import LedgerService
from repository.cartoes_repository import CartoesRepository, listar_destinos_fatura_em_aberto
from repository.categorias_repository import CategoriasRepository
from flowdash_pages.cadastros.cadastro_classes import BancoRepository
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository
from flowdash_pages.lancamentos.shared_ui import canonicalizar_banco  # usado no original

# ---------- Constantes (iguais ao original)
FORMAS = ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "BOLETO"]
ORIGENS_DINHEIRO = ["Caixa", "Caixa 2"]

def _distinct_lower_trim(series: pd.Series) -> list[str]:
    if series is None or series.empty:
        return []
    df = pd.DataFrame({"orig": series.fillna("").astype(str).str.strip()})
    df["key"] = df["orig"].str.lower().str.strip()
    df = df[df["key"] != ""].drop_duplicates("key", keep="first")
    return df["orig"].sort_values().tolist()

def _opcoes_pagamentos(caminho_banco: str, tipo: str) -> list[str]:
    """
    Mesma função do módulo original (Boletos/Empréstimos).
    """
    from shared.db import get_conn
    with get_conn(caminho_banco) as conn:
        if tipo == "Fatura Cartão de Crédito":
            return []

        elif tipo == "Empréstimos e Financiamentos":
            df_emp = pd.read_sql("""
                SELECT DISTINCT
                    TRIM(
                        COALESCE(
                            NULLIF(TRIM(banco),''), NULLIF(TRIM(descricao),''), NULLIF(TRIM(tipo),'')
                        )
                    ) AS rotulo
                FROM emprestimos_financiamentos
            """, conn)
            df_emp = df_emp.dropna()
            df_emp = df_emp[df_emp["rotulo"] != ""]
            return _distinct_lower_trim(df_emp["rotulo"]) if not df_emp.empty else []

        elif tipo == "Boletos":
            df_cart = pd.read_sql("""
                SELECT DISTINCT TRIM(nome) AS nome
                  FROM cartoes_credito
                 WHERE nome IS NOT NULL AND TRIM(nome) <> ''
            """, conn)
            cart_set = set(x.strip().lower() for x in (df_cart["nome"].dropna().tolist() if not df_cart.empty else []))

            df_emp = pd.read_sql("""
                SELECT DISTINCT TRIM(
                    COALESCE(
                        NULLIF(TRIM(banco),''), NULLIF(TRIM(descricao),''), NULLIF(TRIM(tipo),'')
                    )
                ) AS rotulo
                  FROM emprestimos_financiamentos
            """, conn)
            emp_set = set(x.strip().lower() for x in (df_emp["rotulo"].dropna().tolist() if not df_emp.empty else []))

            df_cred = pd.read_sql("""
                SELECT DISTINCT TRIM(credor) AS credor
                  FROM contas_a_pagar_mov
                 WHERE credor IS NOT NULL AND TRIM(credor) <> ''
                   AND COALESCE(status, 'Em aberto') IN ('Em aberto', 'Parcial')
                 ORDER BY credor
            """, conn)

            def eh_boleto_nome(nm: str) -> bool:
                lx = (nm or "").strip().lower()
                return bool(lx) and (lx not in cart_set) and (lx not in emp_set)

            candidatos = [c for c in (df_cred["credor"].dropna().tolist() if not df_cred.empty else []) if eh_boleto_nome(c)]
            if not candidatos:
                return []
            df = pd.DataFrame({"rotulo": candidatos})
            df["key"] = df["rotulo"].str.lower().str.strip()
            df = df[df["key"] != ""].drop_duplicates("key", keep="first")
            return df["rotulo"].sort_values().tolist()

    return []

# ---------------- Resultado
class ResultadoSaida(TypedDict):
    ok: bool
    msg: str

# ---------------- Carregamentos para a UI (listas)
def carregar_listas_para_form(caminho_banco: str):
    """
    Carrega listas necessárias para o formulário.
    Returns: (nomes_bancos, nomes_cartoes, df_categorias, listar_subcategorias_fn, listar_destinos_fatura_em_aberto_fn, carregar_opcoes_pagamentos_fn)
    """
    bancos_repo = BancoRepository(caminho_banco)
    cartoes_repo = CartoesRepository(caminho_banco)
    cats_repo = CategoriasRepository(caminho_banco)

    df_bancos = bancos_repo.carregar_bancos()
    nomes_bancos = df_bancos["nome"].tolist() if df_bancos is not None and not df_bancos.empty else []
    nomes_cartoes = cartoes_repo.listar_nomes()
    df_categorias = cats_repo.listar_categorias()

    return (
        nomes_bancos,
        nomes_cartoes,
        df_categorias,
        cats_repo.listar_subcategorias,            # function
        lambda: listar_destinos_fatura_em_aberto(caminho_banco),  # function
        lambda tipo: _opcoes_pagamentos(caminho_banco, tipo),     # function
    )

# ---------------- Dispatcher principal (mantém as mesmas regras do original)
def registrar_saida(caminho_banco: str, data_lanc: date, usuario_nome: str, payload: dict) -> ResultadoSaida:
    """
    Dispatcher que executa a mesma lógica do módulo original, com as mesmas validações.
    """
    ledger = LedgerService(caminho_banco)
    cartoes_repo = CartoesRepository(caminho_banco)

    # Unpack do payload (nomes idênticos aos usados no original)
    valor_saida = float(payload.get("valor_saida") or 0.0)
    forma_pagamento = (payload.get("forma_pagamento") or "").strip()
    cat_nome = (payload.get("cat_nome") or "").strip()
    subcat_nome = (payload.get("subcat_nome") or "").strip()
    is_pagamentos = bool(payload.get("is_pagamentos"))
    tipo_pagamento_sel = (payload.get("tipo_pagamento_sel") or "").strip() if is_pagamentos else None
    destino_pagamento_sel = (payload.get("destino_pagamento_sel") or "").strip() if is_pagamentos else None

    # Fatura
    competencia_fatura_sel = payload.get("competencia_fatura_sel")
    obrigacao_id_fatura = payload.get("obrigacao_id_fatura")
    multa_fatura = float(payload.get("multa_fatura") or 0.0)
    juros_fatura = float(payload.get("juros_fatura") or 0.0)
    desconto_fatura = float(payload.get("desconto_fatura") or 0.0)

    # Boleto
    parcela_boleto_escolhida = payload.get("parcela_boleto_escolhida")
    multa_boleto = float(payload.get("multa_boleto") or 0.0)
    juros_boleto = float(payload.get("juros_boleto") or 0.0)
    desconto_boleto = float(payload.get("desconto_boleto") or 0.0)

    # Empréstimo
    parcela_emp_escolhida = payload.get("parcela_emp_escolhida")
    multa_emp = float(payload.get("multa_emp") or 0.0)
    juros_emp = float(payload.get("juros_emp") or 0.0)
    desconto_emp = float(payload.get("desconto_emp") or 0.0)

    # Demais campos
    parcelas = int(payload.get("parcelas") or 1)
    cartao_escolhido = (payload.get("cartao_escolhido") or "").strip()
    banco_escolhido_in = (payload.get("banco_escolhido") or "").strip()
    origem_dinheiro = (payload.get("origem_dinheiro") or "").strip()
    venc_1 = payload.get("venc_1")
    fornecedor = (payload.get("fornecedor") or "").strip()
    documento = (payload.get("documento") or "").strip()
    descricao_final = (payload.get("descricao_final") or "").strip()

    data_str = str(data_lanc)

    # ================== Validações gerais/finais ==================
    if is_pagamentos and tipo_pagamento_sel == "Boletos":
        valor_digitado = float(valor_saida)
        if valor_digitado <= 0 and (multa_boleto + juros_boleto - desconto_boleto) <= 0:
            raise ValueError("Informe um valor de pagamento > 0 ou ajustes (multa/juros/desconto).")

    if is_pagamentos and tipo_pagamento_sel == "Fatura Cartão de Crédito":
        valor_digitado = float(valor_saida)
        if valor_digitado <= 0 and (multa_fatura + juros_fatura - desconto_fatura) <= 0:
            raise ValueError("Informe um valor de pagamento > 0 ou ajustes (multa/juros/desconto).")

    if is_pagamentos and tipo_pagamento_sel == "Empréstimos e Financiamentos":
        valor_digitado = float(valor_saida)
        if valor_digitado <= 0 and (multa_emp + juros_emp - desconto_emp) <= 0:
            raise ValueError("Informe um valor de pagamento > 0 ou ajustes (multa/juros/desconto).")

    if not is_pagamentos and valor_saida <= 0:
        raise ValueError("O valor deve ser maior que zero.")

    # Validações específicas dos fluxos
    if forma_pagamento in ["PIX", "DÉBITO"] and not banco_escolhido_in:
        raise ValueError("Selecione ou digite o banco da saída.")
    if forma_pagamento == "DINHEIRO" and not origem_dinheiro:
        raise ValueError("Informe a origem do dinheiro (Caixa/Caixa 2).")

    if is_pagamentos:
        if not tipo_pagamento_sel:
            raise ValueError("Selecione o tipo de pagamento (Fatura, Empréstimos ou Boletos).")
        if tipo_pagamento_sel != "Fatura Cartão de Crédito":
            if not destino_pagamento_sel or not str(destino_pagamento_sel).strip():
                raise ValueError("Selecione o destino correspondente ao tipo escolhido.")
        else:
            if not obrigacao_id_fatura:
                raise ValueError("Selecione uma fatura em aberto (cartão • mês • saldo).")

    # ================== Branches Especiais (Pagamentos) ==================
    if is_pagamentos and tipo_pagamento_sel == "Boletos":
        if not destino_pagamento_sel or not str(destino_pagamento_sel).strip():
            raise ValueError("Selecione o credor do boleto.")
        if not parcela_boleto_escolhida:
            raise ValueError("Selecione a parcela do boleto para pagar (ou informe o identificador).")

        origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)
        id_saida, id_mov, id_cap = ledger.pagar_parcela_boleto(
            data=data_str,
            valor=float(valor_saida),
            forma_pagamento=forma_pagamento,
            origem=origem,
            obrigacao_id=int(payload.get("obrigacao_id") or payload.get("parcela_obrigacao_id", parcela_boleto_escolhida.get("obrigacao_id", 0)) or 0),
            usuario=usuario_nome,
            categoria="Boletos",
            sub_categoria=subcat_nome,
            descricao=descricao_final,
            descricao_extra_cap=f"{destino_pagamento_sel}",
            multa=float(multa_boleto),
            juros=float(juros_boleto),
            desconto=float(desconto_boleto),
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1 or id_mov == -1 or id_cap == -1
                else f"✅ Pagamento de boleto registrado! Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
            ),
        }

    if is_pagamentos and tipo_pagamento_sel == "Fatura Cartão de Crédito":
        origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)
        id_saida, id_mov, id_cap = ledger.pagar_fatura_cartao(
            data=data_str,
            valor=float(valor_saida),
            forma_pagamento=forma_pagamento,
            origem=origem,
            obrigacao_id=int(obrigacao_id_fatura),
            usuario=usuario_nome,
            categoria="Fatura Cartão de Crédito",
            sub_categoria=subcat_nome,
            descricao=descricao_final,
            multa=float(multa_fatura),
            juros=float(juros_fatura),
            desconto=float(desconto_fatura),
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1 or id_mov == -1 or id_cap == -1
                else f"✅ Pagamento de fatura registrado! Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
            ),
        }

    if is_pagamentos and tipo_pagamento_sel == "Empréstimos e Financiamentos":
        if not destino_pagamento_sel:
            raise ValueError("Selecione o banco/descrição do empréstimo.")
        if not parcela_emp_escolhida:
            raise ValueError("Selecione a parcela do empréstimo (ou informe o identificador).")

        origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)
        id_saida, id_mov, id_cap = ledger.pagar_parcela_emprestimo(
            data=data_str,
            valor=float(valor_saida),
            forma_pagamento=forma_pagamento,
            origem=origem,
            obrigacao_id=int(payload.get("obrigacao_id") or payload.get("parcela_obrigacao_id", 0) or 0),
            usuario=usuario_nome,
            categoria="Empréstimos e Financiamentos",
            sub_categoria=subcat_nome,
            descricao=descricao_final,
            multa=float(multa_emp),
            juros=float(juros_emp),
            desconto=float(desconto_emp),
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1 or id_mov == -1 or id_cap == -1
                else f"✅ Parcela de Empréstimo paga! Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
            ),
        }

    # ================== Fluxos Padrão ==================
    categoria = cat_nome
    sub_categoria = subcat_nome

    if forma_pagamento == "DINHEIRO":
        id_saida, id_mov = ledger.registrar_saida_dinheiro(
            data=data_str,
            valor=float(valor_saida),
            origem_dinheiro=origem_dinheiro,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao_final,
            usuario=usuario_nome,
        )
        return {
            "ok": True,
            "msg": ("ℹ️ Transação já registrada (idempotência)." if id_saida == -1 else
                    f"✅ Saída em dinheiro registrada! ID saída: {id_saida} | Log: {id_mov}")
        }

    if forma_pagamento in ["PIX", "DÉBITO"]:
        banco_nome = _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)
        id_saida, id_mov = ledger.registrar_saida_bancaria(
            data=data_str,
            valor=float(valor_saida),
            banco_nome=banco_nome,
            forma=forma_pagamento,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao_final,
            usuario=usuario_nome,
        )
        return {
            "ok": True,
            "msg": ("ℹ️ Transação já registrada (idempotência)." if id_saida == -1 else
                    f"✅ Saída bancária ({forma_pagamento}) registrada! ID saída: {id_saida} | Log: {id_mov}")
        }

    if forma_pagamento == "CRÉDITO":
        fc_vc = cartoes_repo.obter_por_nome(cartao_escolhido)
        if not fc_vc:
            raise ValueError("Cartão não encontrado. Cadastre em 📇 Cartão de Crédito.")
        vencimento, fechamento = fc_vc  # ordem preservada
        ids_fatura, id_mov = ledger.registrar_saida_credito(
            data_compra=data_str,
            valor=float(valor_saida),
            parcelas=int(parcelas),
            cartao_nome=cartao_escolhido,
            categoria=categoria,
            sub_categoria=subcat_nome,
            descricao=descricao_final,
            usuario=usuario_nome,
            fechamento=int(fechamento),
            vencimento=int(vencimento),
        )
        return {
            "ok": True,
            "msg": ("ℹ️ Transação já registrada (idempotência)." if not ids_fatura else
                    f"✅ Despesa em CRÉDITO programada! Parcelas criadas: {len(ids_fatura)} | Log: {id_mov}")
        }

    if forma_pagamento == "BOLETO":
        ids_cap, id_mov = ledger.registrar_saida_boleto(
            data_compra=data_str,
            valor=float(valor_saida),
            parcelas=int(parcelas),
            vencimento_primeira=str(payload.get("venc_1")),
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao_final,
            usuario=usuario_nome,
            fornecedor=(payload.get("fornecedor") or None),
            documento=(payload.get("documento") or None),
        )
        return {
            "ok": True,
            "msg": ("ℹ️ Transação já registrada (idempotência)." if not ids_cap else
                    f"✅ Boleto programado! Parcelas criadas: {len(ids_cap)} | Log: {id_mov}")
        }

    # Se chegou aqui, forma desconhecida
    raise ValueError("Forma de pagamento inválida ou não suportada.")

# ---------------- Canonicalização de banco (igual ao original, tolerante a falha)
def _canonicalizar_banco_safe(caminho_banco: str, banco_in: str) -> str:
    try:
        return canonicalizar_banco(caminho_banco, (banco_in or "").strip()) or (banco_in or "").strip()
    except Exception:
        return (banco_in or "").strip()