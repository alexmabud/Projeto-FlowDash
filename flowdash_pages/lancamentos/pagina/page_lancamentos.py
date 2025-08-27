"""
Página agregadora de **Lançamentos**: exibe o resumo do dia e renderiza as subpáginas
(Venda, Saída, Caixa 2, Depósito, Transferência e Mercadorias).
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
import importlib
from typing import Any, Callable, Optional

import pandas as pd
import streamlit as st

from .actions_pagina import carregar_resumo_dia
from .ui_cards_pagina import render_card_row, render_card_mercadorias


# ===================== Helpers =====================
def _get_default_data_lanc() -> Optional[str]:
    """Obtém ou inicializa `data_lanc` no session_state como string YYYY-MM-DD.

    Returns:
        Data no formato "YYYY-MM-DD" ou None em caso de falha.
    """
    try:
        v = st.session_state.get("data_lanc")
        if not v:
            v = date.today().strftime("%Y-%m-%d")
            st.session_state["data_lanc"] = v
        return v
    except Exception:
        return None


def _brl(v: float | int | None) -> str:
    """Formata um número em BRL sem depender de locale.

    Args:
        v: Valor numérico.

    Returns:
        String no formato "R$ 1.234,56".
    """
    try:
        n = float(v or 0.0)
    except Exception:
        n = 0.0
    return f"R$ {n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _safe_call(mod_path: str, func_name: str, state: Any) -> None:
    """Importa e executa com segurança `func_name(state)` do módulo `mod_path`.

    Mostra aviso/erro amigável e evita derrubar a página em caso de falha.

    Args:
        mod_path: Caminho do módulo (ex.: 'flowdash_pages.lancamentos.venda.page_venda').
        func_name: Nome da função exportada (ex.: 'render_venda').
        state: Objeto de estado a ser repassado para a função.
    """
    try:
        mod = importlib.import_module(mod_path)
        fn: Optional[Callable[[Any], None]] = getattr(mod, func_name, None)
        if fn is None:
            st.warning(f"⚠️ A subpágina '{mod_path}' não expõe `{func_name}(state)`.")
            return
        fn(state)
    except Exception as e:
        st.error(f"❌ Falha ao renderizar '{mod_path}.{func_name}': {e}")


# ===================== Page =====================
def render_page(caminho_banco: str, data_default: date | None = None) -> None:
    """Renderiza a página agregadora de Lançamentos.

    Args:
        caminho_banco: Caminho do arquivo SQLite.
        data_default: Data inicial do input (padrão = hoje).
    """
    # Mensagem de sucesso de operações anteriores
    if "msg_ok" in st.session_state:
        st.success(st.session_state.pop("msg_ok"))

    # Data de referência do lançamento
    data_lanc = st.date_input(
        "🗓️ Data do Lançamento",
        value=data_default or date.today(),
        key="data_lanc",
    )
    st.markdown(f"## 🧾 Lançamentos do Dia — **{data_lanc}**")

    # Resumo agregado do dia
    resumo = carregar_resumo_dia(caminho_banco, data_lanc) or {}

    # ----- Resumo do Dia -----
    total_vendas = float(resumo.get("total_vendas", 0.0))
    total_saidas = float(resumo.get("total_saidas", 0.0))
    render_card_row(
        "📊 Resumo do Dia",
        [("Vendas", total_vendas, True), ("Saídas", total_saidas, True)],
    )

    # ----- Saldos -----
    saldos_bancos = resumo.get("saldos_bancos") or {}
    nb = {(str(k) or "").strip().lower(): float(v or 0.0) for k, v in saldos_bancos.items()}
    inter = nb.get("inter", 0.0)
    infinite = nb.get("infinitepay", nb.get("infinitiepay", nb.get("infinite pay", 0.0)))
    bradesco = nb.get("bradesco", 0.0)

    render_card_row(
        "💵 Saldos",
        [
            ("Caixa", resumo.get("caixa_total", 0.0), True),
            ("Caixa 2", resumo.get("caixa2_total", 0.0), True),
            ("Inter", inter, True),
            ("InfinitePay", infinite, True),
            ("Bradesco", bradesco, True),
        ],
    )

    # ----- Transferências (card com 3 colunas) -----
    # 1) P/ Caixa 2 (número)
    transf_caixa2_total = float(resumo.get("transf_caixa2_total", 0.0))

    # 2) Depósitos (lista)
    dep_lin: list[str] = []
    for b, v in (resumo.get("depositos_list") or []):
        dep_lin.append(f"{_brl(v)} → {b or '—'}")

    # 3) Transferência entre bancos — TABELA real (Valor | Saída | Entrada)
    trf_raw = resumo.get("transf_bancos_list") or []  # List[Tuple[origem, destino, valor]]
    trf_df = pd.DataFrame(trf_raw, columns=["Saída", "Entrada", "Valor"])
    trf_df["Saída"] = trf_df["Saída"].fillna("").str.strip().replace("", "—")
    trf_df["Entrada"] = trf_df["Entrada"].fillna("").str.strip().replace("", "—")
    trf_df["Valor"] = pd.to_numeric(trf_df["Valor"], errors="coerce").fillna(0.0)
    trf_df = trf_df[["Valor", "Saída", "Entrada"]]  # ordem exata solicitada

    render_card_row(
        "🔁 Transferências",
        [
            ("P/ Caixa 2", transf_caixa2_total, False),
            ("Depósito Bancário", dep_lin, False),
            ("Transferência entre bancos", trf_df, False),
        ],
    )

    # ----- Mercadorias -----
    render_card_mercadorias(resumo.get("compras_list") or [], resumo.get("receb_list") or [])

    # ----- Ações (subpáginas) -----
    state = SimpleNamespace(db_path=caminho_banco, caminho_banco=caminho_banco, data_lanc=data_lanc)
    st.markdown("### ➕ Ações")
    a1, a2 = st.columns(2)
    with a1:
        _safe_call("flowdash_pages.lancamentos.venda.page_venda", "render_venda", state)
    with a2:
        _safe_call("flowdash_pages.lancamentos.saida.page_saida", "render_saida", state)

    c1, c2, c3 = st.columns(3)
    with c1:
        _safe_call("flowdash_pages.lancamentos.caixa2.page_caixa2", "render_caixa2", state)
    with c2:
        _safe_call("flowdash_pages.lancamentos.deposito.page_deposito", "render_deposito", state)
    with c3:
        _safe_call("flowdash_pages.lancamentos.transferencia.page_transferencia", "render_transferencia", state)

    st.markdown("---")
    st.markdown("### 📦 Mercadorias — Lançamentos")
    _safe_call("flowdash_pages.lancamentos.mercadorias.page_mercadorias", "render_mercadorias", state)
