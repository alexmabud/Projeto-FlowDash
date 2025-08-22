# ===================== Page: Lançamentos (Agregadora) =====================
"""
Página principal que exibe o **resumo do dia** e renderiza as subpáginas:
Venda, Saída, Caixa 2, Depósito, Transferência e Mercadorias.
Toda a regra do arquivo original foi mantida; apenas organizada.
"""

from __future__ import annotations
import streamlit as st
from datetime import date

from .actions_pagina import carregar_resumo_dia
from .ui_cards_pagina import render_card_row, render_card_mercadorias

# Subpáginas (novo padrão)
from ..venda.page_venda import render_page as render_venda
from ..saida.page_saida import render_page as render_saida
from ..caixa2.page_caixa2 import render_page as render_caixa2
from ..deposito.page_deposito import render_page as render_deposito
from ..transferencia.page_transferencia import render_page as render_transferencia_bancaria
from ..mercadorias.page_mercadorias import render_page as render_mercadorias

def render_page(caminho_banco: str, data_default: date | None = None):
    """
    Renderiza a página agregadora de Lançamentos.

    Args:
        caminho_banco: caminho do SQLite.
        data_default: data inicial do input (padrão = hoje).
    """
    # Mensagens de sucesso vindas de subpáginas
    if "msg_ok" in st.session_state:
        st.success(st.session_state.pop("msg_ok"))

    # Data de referência
    data_lanc = st.date_input("🗓️ Data do Lançamento", value=data_default or date.today(), key="data_lanc")
    st.markdown(f"## 🧾 Lançamentos do Dia — **{data_lanc}**")

    # ===== Resumo (iguais ao original) =====
    resumo = carregar_resumo_dia(caminho_banco, data_lanc)

    render_card_row("📊 Resumo do Dia", [
        ("Vendas", resumo["total_vendas"], True),
        ("Saídas", resumo["total_saidas"], True),
    ])

    nb = { (k or "").strip().lower(): float(v or 0.0) for k, v in (resumo["saldos_bancos"] or {}).items() }
    inter    = nb.get("inter", 0.0)
    infinite = nb.get("infinitepay", nb.get("infinitiepay", nb.get("infinite pay", 0.0)))
    bradesco = nb.get("bradesco", 0.0)
    render_card_row("💵 Saldos", [
        ("Caixa",       resumo["caixa_total"],  True),
        ("Caixa 2",     resumo["caixa2_total"], True),
        ("Inter",       inter,                  True),
        ("InfinitePay", infinite,               True),
        ("Bradesco",    bradesco,               True),
    ])

    dep_lin = [f"R$ {float(v or 0.0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + f" → {(b or '—')}"
               for (b, v) in (resumo["depositos_list"] or [])]
    trf_lin = []
    for de, para, v in (resumo["transf_bancos_list"] or []):
        de_txt = (de or "").strip()
        val_txt = f"R$ {float(v or 0.0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        trf_lin.append(f"{val_txt} {'%s ' % de_txt if de_txt else ''}→ {(para or '—')}")
    render_card_row("🔁 Transferências", [
        ("P/ Caixa 2",                 resumo["transf_caixa2_total"], False),
        ("Depósito Bancário",          dep_lin,                        False),
        ("Transferência entre bancos", trf_lin,                        False),
    ])

    render_card_mercadorias(resumo["compras_list"], resumo["receb_list"])

    # ===== Ações (mesmo layout do original) =====
    st.markdown("### ➕ Ações")
    a1, a2 = st.columns(2)
    with a1:
        render_venda(caminho_banco, data_lanc)
    with a2:
        render_saida(caminho_banco, data_lanc)

    c1, c2, c3 = st.columns(3)
    with c1:
        render_caixa2(caminho_banco, data_lanc)
    with c2:
        render_deposito(caminho_banco, data_lanc)
    with c3:
        render_transferencia_bancaria(caminho_banco, data_lanc)

    st.markdown("---")
    st.markdown("### 📦 Mercadorias — Lançamentos")
    render_mercadorias(caminho_banco, data_lanc)
