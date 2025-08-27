# ===================== UI Forms: Transferência =====================
"""
Apenas UI — sem regra/SQL.

Campos:
    - Banco de origem
    - Banco de destino
    - Valor
"""

from __future__ import annotations

from datetime import date
from typing import Any, Callable, Dict, List, Sequence

import streamlit as st


def render_form_transferencia(
    data_lanc: date,
    nomes_bancos: Sequence[str],
    invalidate_cb: Callable[[], None],
) -> Dict[str, Any]:
    """Renderiza o formulário de Transferência (somente UI).

    Args:
        data_lanc: Data do lançamento.
        nomes_bancos: Lista/sequence com nomes de bancos disponíveis.
        invalidate_cb: Callback a ser chamado em mudanças de campos críticos
            (ideal para invalidar confirmação no `session_state`).

    Returns:
        Dicionário com os campos preenchidos:
            {
                "banco_origem": str,
                "banco_destino": str,
                "valor": float,
                "confirmado": bool,
                "salvar": False,     # compatibilidade com páginas antigas
                "cancelar": False,   # compatibilidade com páginas antigas
            }
    """
    with st.container():
        st.markdown("#### 🔁 Transferência entre Bancos")
        st.caption(f"Data do lançamento: **{data_lanc.strftime('%d/%m/%Y')}**")

        c1, c2 = st.columns(2)
        with c1:
            if nomes_bancos:
                banco_origem = st.selectbox(
                    "Banco de Origem",
                    options=list(nomes_bancos),
                    key="trf_banco_origem",
                    on_change=invalidate_cb,
                )
            else:
                banco_origem = st.text_input(
                    "Banco de Origem (digite)",
                    key="trf_banco_origem_txt",
                    on_change=invalidate_cb,
                )

        with c2:
            if nomes_bancos:
                banco_destino = st.selectbox(
                    "Banco de Destino",
                    options=list(nomes_bancos),
                    key="trf_banco_destino",
                    on_change=invalidate_cb,
                )
            else:
                banco_destino = st.text_input(
                    "Banco de Destino (digite)",
                    key="trf_banco_destino_txt",
                    on_change=invalidate_cb,
                )

        valor = st.number_input(
            "Valor da Transferência",
            min_value=0.0,
            step=0.01,
            format="%.2f",
            key="trf_valor",
            on_change=invalidate_cb,
        )

        # Resumo (somente UI)
        st.info(
            "\n".join(
                [
                    "**Confirme os dados da transferência**",
                    f"- **Data:** {data_lanc.strftime('%d/%m/%Y')}",
                    f"- **Origem:** {banco_origem or '—'}",
                    f"- **Destino:** {banco_destino or '—'}",
                    f"- **Valor:** R$ {valor:.2f}",
                ]
            )
        )

        # Confirmação (não dispara ação — apenas UI/estado)
        confirmado = st.checkbox(
            "Está tudo certo com os dados acima?",
            key="transferencia_confirmada",
        )

    return {
        "banco_origem": (banco_origem or "").strip(),
        "banco_destino": (banco_destino or "").strip(),
        "valor": float(valor or 0.0),
        "confirmado": bool(confirmado),
        # compat: mantidos para páginas antigas (não acionam nada aqui)
        "salvar": False,
        "cancelar": False,
    }
