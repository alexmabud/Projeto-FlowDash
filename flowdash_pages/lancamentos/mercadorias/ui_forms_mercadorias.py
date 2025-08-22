# ===================== UI Forms: Mercadorias =====================
"""
Componentes de UI (somente interface, sem SQL).

"""

from __future__ import annotations

import streamlit as st
from datetime import date
from utils.utils import formatar_valor

# --------- helpers UI locais (sem DB)
def _to_float_or_none(x):
    if x is None:
        return None
    try:
        s = str(x).strip().replace(",", ".")
        return float(s) if s != "" else None
    except Exception:
        return None

# ---------- Formulário: Compra
def render_form_compra(data_lanc: date):
    st.markdown("#### 🧾 Compra de Mercadorias")

    # Linha 1
    c1, c2, c3 = st.columns([1, 1, 1.4])
    with c1:
        st.text_input("Data (YYYY-MM-DD)", value=str(data_lanc), disabled=True, key="merc_compra_data_display")
        data_txt = str(data_lanc)
    with c2:
        colecao = st.text_input("Coleção", key="merc_compra_colecao")
    with c3:
        fornecedor = st.text_input("Fornecedor", key="merc_compra_fornecedor")

    # Linha 2
    c4, c5, c6, c7 = st.columns([1, 1, 1, 1])
    with c4:
        valor_mercadoria = st.number_input("Valor da Mercadoria (R$)", min_value=0.0, step=0.01, key="merc_compra_valor")
    with c5:
        frete = st.number_input("Frete (R$)", min_value=0.0, step=0.01, key="merc_compra_frete")
    with c6:
        forma_opts = ["PIX", "BOLETO", "CRÉDITO", "DÉBITO", "DINHEIRO", "OUTRO"]
        forma_sel = st.selectbox("Forma de Pagamento", forma_opts, key="merc_compra_forma_sel")
    with c7:
        parcelas = st.number_input("Parcelas", min_value=1, max_value=360, step=1, value=1, key="merc_compra_parcelas")

    forma_pagamento = (
        st.text_input("Informe a forma de pagamento (OUTRO)", key="merc_compra_forma_outro").strip().upper()
        if forma_sel == "OUTRO" else forma_sel
    )
    if forma_pagamento == "CRÉDITO":
        st.caption(f"Parcelas: **{int(parcelas)}×**")

    # Linha 3 – Previsões
    st.markdown("###### Previsões")
    p1, p2 = st.columns(2)
    with p1:
        prev_fat_dt = st.date_input("Previsão de Faturamento", value=data_lanc, key="merc_compra_prev_fat_dt")
    with p2:
        prev_rec_dt = st.date_input("Previsão de Recebimento", value=data_lanc, key="merc_compra_prev_rec_dt")

    # Linha 4 – N°s
    n1, n2 = st.columns(2)
    with n1:
        numero_pedido_str = st.text_input("Número do Pedido", key="merc_compra_num_pedido")
    with n2:
        numero_nf_str = st.text_input("Número da Nota Fiscal", key="merc_compra_num_nf")

    confirmado = st.checkbox("Confirmo os dados", key="merc_compra_confirma_out")

    return {
        "data_txt": data_txt,
        "colecao": (colecao or "").strip(),
        "fornecedor": (fornecedor or "").strip(),
        "valor_mercadoria": float(valor_mercadoria or 0.0),
        "frete": _to_float_or_none(frete),
        "forma_pagamento": (forma_pagamento or "").strip().upper(),
        "parcelas": int(parcelas or 1),
        "prev_fat_dt": str(prev_fat_dt) if prev_fat_dt else None,
        "prev_rec_dt": str(prev_rec_dt) if prev_rec_dt else None,
        "numero_pedido": _to_float_or_none(numero_pedido_str),
        "numero_nf": _to_float_or_none(numero_nf_str),
        "confirmado": bool(confirmado),
    }

# ---------- Formulário: Recebimento
def render_form_recebimento(data_lanc: date, compras_options: list[dict], selected_id: int | None):
    st.markdown("#### 📥 Recebimento de Mercadorias")

    # Cabeçalho da compra selecionada
    sel = next((c for c in compras_options if c["id"] == selected_id), None)
    if not sel:
        st.warning("Seleção inválida.")
        return None

    # Blocos
    b1, b2, b3 = st.columns([1, 1, 1.4])
    with b1:
        st.text_input("Data da Compra", value=sel["Data"], disabled=True)
    with b2:
        st.text_input("Coleção", value=sel["Colecao"], disabled=True)
    with b3:
        st.text_input("Fornecedor", value=sel["Fornecedor"], disabled=True)

    b4, b5 = st.columns(2)
    with b4:
        st.text_input("Previsão de Faturamento", value=sel["PrevFat"], disabled=True)
    with b5:
        st.text_input("Previsão de Recebimento", value=sel["PrevRec"], disabled=True)

    v1, v2 = st.columns(2)
    with v1:
        st.text_input("Valor da Mercadoria (pedido)", value=formatar_valor(sel["Valor_Mercadoria"]), disabled=True)
    with v2:
        st.text_input("Frete (pedido)", value=formatar_valor(sel["Frete"]), disabled=True)

    n1, n2 = st.columns(2)
    with n1:
        numero_pedido_txt = st.text_input("Número do Pedido (editável)", value=sel["Numero_Pedido"], key="merc_receb_edit_pedido")
    with n2:
        numero_nf_txt = st.text_input("Número da Nota Fiscal (editável)", value=sel["Numero_NF"], key="merc_receb_edit_nf")

    st.markdown("###### Informe os dados efetivos e divergências (se houver)")
    e1, e2 = st.columns(2)
    with e1:
        fat_dt = st.date_input("Faturamento (efetivo)", value=data_lanc, key="merc_receb_fat_dt")
    with e2:
        rec_dt = st.date_input("Recebimento (efetivo)", value=data_lanc, key="merc_receb_rec_dt")

    d1, d2 = st.columns(2)
    with d1:
        valor_recebido = st.number_input("Valor Recebido (R$)", min_value=0.0, step=0.01, key="merc_receb_valor_recebido")
    with d2:
        frete_cobrado = st.number_input("Frete Cobrado (R$)", min_value=0.0, step=0.01, key="merc_receb_frete_cobrado")

    obs = st.text_area(
        "Observações (divergências, avarias, diferenças de quantidade etc.)",
        key="merc_receb_obs",
        placeholder="Opcional"
    )

    confirmado = st.checkbox("Confirmo os dados", key="merc_receb_confirma_out")

    return {
        "selected_id": int(sel["id"]),
        "fat_dt": str(fat_dt) if fat_dt else None,
        "rec_dt": str(rec_dt) if rec_dt else None,
        "valor_recebido": _to_float_or_none(valor_recebido),
        "frete_cobrado": _to_float_or_none(frete_cobrado),
        "obs": (obs or None),
        "numero_pedido": (numero_pedido_txt.strip() or None),
        "numero_nf": (numero_nf_txt.strip() or None),
        "confirmado": bool(confirmado),
    }
