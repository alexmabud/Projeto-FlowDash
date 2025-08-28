# ===================== UI Forms: Mercadorias =====================
"""Componentes de UI (somente interface, sem SQL).

Este módulo contém os formulários de **Compra** e **Recebimento** de mercadorias.
Ele não executa operações de banco de dados; apenas coleta e normaliza os dados
de entrada do usuário.

"""

from __future__ import annotations

from datetime import date

import streamlit as st

from utils import formatar_valor  


# ---------- CSS: remove bordas/caixas dos wrappers ----------
def _inject_form_css() -> None:
    """Injeta CSS para remover bordas/caixas de wrappers de formulário na página."""
    st.markdown(
        """
        <style>
        /* Remove borda e fundo do st.form */
        div[data-testid="stForm"] {
          border: 0 !important;
          padding: 0 !important;
          background: transparent !important;
          box-shadow: none !important;
        }
        /* Remove borda/fundo do container com borda */
        div[data-testid="stContainer"] > div:has(> div[style*="border"]){
          border: 0 !important;
          background: transparent !important;
          box-shadow: none !important;
        }
        /* Remove borda do expander */
        div[data-testid="stExpander"] {
          border: 0 !important;
          background: transparent !important;
          box-shadow: none !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# --------- helpers UI locais (sem DB)
def _to_float_or_none(x) -> float | None:
    """Converte entrada para float, retornando None quando vazio/inválido.

    Args:
        x: Valor de entrada (qualquer tipo).

    Returns:
        Float convertido ou None quando vazio/inválido.
    """
    if x is None:
        return None
    try:
        s = str(x).strip().replace(",", ".")
        return float(s) if s != "" else None
    except Exception:
        return None


def _safe_str(x) -> str:
    """Retorna string segura para inputs de texto.

    Converte None em string vazia para evitar erros em `st.text_input`.

    Args:
        x: Valor de entrada.

    Returns:
        String representando `x`, ou "" quando `x` é None.
    """
    return "" if x is None else str(x)


def _to_str_or_none(x) -> str | None:
    """Normaliza retorno de campos de texto editáveis.

    Converte para string, faz `strip()` e retorna None quando ficar vazio.

    Args:
        x: Valor de entrada.

    Returns:
        String normalizada ou None.
    """
    if x is None:
        return None
    s = str(x).strip()
    return s if s != "" else None


# ---------- Formulário: Compra (sem headers/bordas)
def render_form_compra(data_lanc: date) -> dict:
    """Renderiza o formulário de **Compra de Mercadorias**.

    Atenção:
        Este formulário apenas coleta dados. A validação/habilitação do botão
        “Salvar” deve ser feita na página chamadora.

    Args:
        data_lanc: Data padrão do lançamento (exibida e desabilitada).

    Returns:
        Dict com os campos:
            - data_txt (str)
            - colecao (str)
            - fornecedor (str)
            - valor_mercadoria (float)
            - frete (float | None)
            - forma_pagamento (str)
            - parcelas (int)
            - prev_fat_dt (str | None)
            - prev_rec_dt (str | None)
            - numero_pedido (float | None)   # mantém comportamento original
            - numero_nf (float | None)       # mantém comportamento original
            - confirmado (bool)
    """
    _inject_form_css()  # garante estilo "solto" mesmo se a página usar form/expander

    # Linha 1
    c1, c2, c3 = st.columns([1, 1, 1.4])
    with c1:
        st.text_input(
            "Data (YYYY-MM-DD)",
            value=str(data_lanc),
            disabled=True,
            key="merc_compra_data_display",
        )
        data_txt = str(data_lanc)
    with c2:
        colecao = st.text_input("Coleção", key="merc_compra_colecao")
    with c3:
        fornecedor = st.text_input("Fornecedor", key="merc_compra_fornecedor")

    # Linha 2
    c4, c5, c6, c7 = st.columns([1, 1, 1, 1])
    with c4:
        valor_mercadoria = st.number_input(
            "Valor da Mercadoria (R$)",
            min_value=0.0,
            step=0.01,
            key="merc_compra_valor",
        )
    with c5:
        frete = st.number_input(
            "Frete (R$)",
            min_value=0.0,
            step=0.01,
            key="merc_compra_frete",
        )
    with c6:
        forma_opts = ["PIX", "BOLETO", "CRÉDITO", "DÉBITO", "DINHEIRO", "OUTRO"]
        forma_sel = st.selectbox("Forma de Pagamento", forma_opts, key="merc_compra_forma_sel")
    with c7:
        parcelas = st.number_input(
            "Parcelas",
            min_value=1,
            max_value=360,
            step=1,
            value=1,
            key="merc_compra_parcelas",
        )

    forma_pagamento = (
        st.text_input(
            "Informe a forma de pagamento (OUTRO)", key="merc_compra_forma_outro"
        ).strip().upper()
        if forma_sel == "OUTRO"
        else forma_sel
    )
    if forma_pagamento == "CRÉDITO":
        st.caption(f"Parcelas: {int(parcelas)}×")

    # Linha 3 – Previsões
    p1, p2 = st.columns(2)
    with p1:
        prev_fat_dt = st.date_input(
            "Previsão de Faturamento", value=data_lanc, key="merc_compra_prev_fat_dt"
        )
    with p2:
        prev_rec_dt = st.date_input(
            "Previsão de Recebimento", value=data_lanc, key="merc_compra_prev_rec_dt"
        )

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


# ---------- Formulário: Recebimento (sem headers/bordas)
def render_form_recebimento(
    data_lanc: date, compras_options: list[dict], selected_id: int | None
) -> dict | None:
    """Renderiza o formulário de **Recebimento de Mercadorias**.

    Args:
        data_lanc: Data padrão sugerida para faturamento/recebimento efetivos.
        compras_options: Lista de compras para selecionar a que será recebida.
        selected_id: ID da compra selecionada.

    Returns:
        Dict com os campos normalizados ou None quando a seleção for inválida:
            - selected_id (int)
            - fat_dt (str | None)
            - rec_dt (str | None)
            - valor_recebido (float | None)
            - frete_cobrado (float | None)
            - obs (str | None)
            - numero_pedido (str | None)
            - numero_nf (str | None)
            - confirmado (bool)
    """
    _inject_form_css()  # garante estilo "solto" mesmo se a página usar form/expander

    # Cabeçalho da compra selecionada
    sel = next((c for c in compras_options if c["id"] == selected_id), None)
    if not sel:
        st.warning("Seleção inválida.")
        return None

    # Blocos — usar valores SEGUROS ('' quando None) nos text_input
    b1, b2, b3 = st.columns([1, 1, 1.4])
    with b1:
        st.text_input("Data da Compra", value=_safe_str(sel.get("Data")), disabled=True)
    with b2:
        st.text_input("Coleção", value=_safe_str(sel.get("Colecao")), disabled=True)
    with b3:
        st.text_input("Fornecedor", value=_safe_str(sel.get("Fornecedor")), disabled=True)

    b4, b5 = st.columns(2)
    with b4:
        st.text_input(
            "Previsão de Faturamento", value=_safe_str(sel.get("PrevFat")), disabled=True
        )
    with b5:
        st.text_input(
            "Previsão de Recebimento", value=_safe_str(sel.get("PrevRec")), disabled=True
        )

    v1, v2 = st.columns(2)
    with v1:
        st.text_input(
            "Valor da Mercadoria (pedido)",
            value=formatar_valor(sel.get("Valor_Mercadoria")),
            disabled=True,
        )
    with v2:
        st.text_input(
            "Frete (pedido)",
            value=formatar_valor(sel.get("Frete")),
            disabled=True,
        )

    n1, n2 = st.columns(2)
    with n1:
        numero_pedido_txt = st.text_input(
            "Número do Pedido (editável)",
            value=_safe_str(sel.get("Numero_Pedido")),
            key="merc_receb_edit_pedido",
        )
    with n2:
        numero_nf_txt = st.text_input(
            "Número da Nota Fiscal (editável)",
            value=_safe_str(sel.get("Numero_NF")),
            key="merc_receb_edit_nf",
        )

    e1, e2 = st.columns(2)
    with e1:
        fat_dt = st.date_input(
            "Faturamento (efetivo)", value=data_lanc, key="merc_receb_fat_dt"
        )
    with e2:
        rec_dt = st.date_input(
            "Recebimento (efetivo)", value=data_lanc, key="merc_receb_rec_dt"
        )

    d1, d2 = st.columns(2)
    with d1:
        valor_recebido = st.number_input(
            "Valor Recebido (R$)", min_value=0.0, step=0.01, key="merc_receb_valor_recebido"
        )
    with d2:
        frete_cobrado = st.number_input(
            "Frete Cobrado (R$)", min_value=0.0, step=0.01, key="merc_receb_frete_cobrado"
        )

    obs = st.text_area(
        "Observações (divergências, avarias, diferenças de quantidade etc.)",
        key="merc_receb_obs",
        placeholder="Opcional",
    )

    confirmado = st.checkbox("Confirmo os dados", key="merc_receb_confirma_out")

    return {
        "selected_id": int(sel["id"]),
        "fat_dt": str(fat_dt) if fat_dt else None,
        "rec_dt": str(rec_dt) if rec_dt else None,
        "valor_recebido": _to_float_or_none(valor_recebido),
        "frete_cobrado": _to_float_or_none(frete_cobrado),
        "obs": _to_str_or_none(obs),
        "numero_pedido": _to_str_or_none(numero_pedido_txt),
        "numero_nf": _to_str_or_none(numero_nf_txt),
        "confirmado": bool(confirmado),
    }
