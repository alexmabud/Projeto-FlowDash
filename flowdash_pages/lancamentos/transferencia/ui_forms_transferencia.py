# ===================== UI Forms: Transferência =====================
"""
Apenas UI – sem regra/SQL. Campos:
- Banco de origem
- Banco de destino
- Valor
- Observação (opcional)
"""

from __future__ import annotations

import streamlit as st
from datetime import date

def render_form_transferencia(
    data_lanc: date,
    nomes_bancos: list[str],
    invalidate_cb,
) -> dict:
    st.markdown("#### 🔁 Transferência entre Bancos")
    st.caption(f"Data do lançamento: **{data_lanc}**")

    c1, c2 = st.columns(2)
    with c1:
        banco_origem = (
            st.selectbox("Banco de Origem", nomes_bancos, key="trf_banco_origem", on_change=invalidate_cb)
            if nomes_bancos else
            st.text_input("Banco de Origem (digite)", key="trf_banco_origem_txt", on_change=invalidate_cb)
        )
    with c2:
        banco_destino = (
            st.selectbox("Banco de Destino", nomes_bancos, key="trf_banco_destino", on_change=invalidate_cb)
            if nomes_bancos else
            st.text_input("Banco de Destino (digite)", key="trf_banco_destino_txt", on_change=invalidate_cb)
        )

    valor = st.number_input("Valor da Transferência", min_value=0.0, step=0.01, format="%.2f",
                            key="trf_valor", on_change=invalidate_cb)
    observacao = st.text_input("Observação (opcional)", key="trf_obs")

    # Resumo
    st.info("\n".join([
        "**Confirme os dados da transferência**",
        f"- **Data:** {data_lanc.strftime('%d/%m/%Y')}",
        f"- **Origem:** {banco_origem or '—'}",
        f"- **Destino:** {banco_destino or '—'}",
        f"- **Valor:** R$ {valor:.2f}",
        f"- **Obs.:** {observacao or '—'}",
    ]))

    confirmado = st.checkbox("Está tudo certo com os dados acima?", key="transferencia_confirmada")

    return {
        "banco_origem": (banco_origem or "").strip(),
        "banco_destino": (banco_destino or "").strip(),
        "valor": float(valor or 0.0),
        "observacao": (observacao or "").strip(),
        "confirmado": bool(confirmado),
    }
