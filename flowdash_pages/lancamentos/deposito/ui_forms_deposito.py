# ===================== UI Forms: Depósito =====================
"""
Componentes de UI para Depósito. Apenas interface – sem regra/SQL.
"""

import streamlit as st
import pandas as pd
from utils.utils import formatar_valor

def render_form(data_lanc, nomes_bancos: list[str]) -> dict:
    """
    Desenha o formulário de depósito.

    Args:
        data_lanc: data do lançamento.
        nomes_bancos: lista de bancos cadastrados.

    Returns:
        dict: {"valor": float, "banco_escolhido": str, "confirmado": bool, "submit": bool}
    """
    st.markdown("#### 🏦 Depósito de Caixa 2 no Banco")
    st.caption(f"Data do lançamento: **{pd.to_datetime(data_lanc).strftime('%d/%m/%Y')}**")

    col_a, col_b = st.columns(2)
    with col_a:
        valor = st.number_input(
            "Valor do Depósito",
            min_value=0.0, step=0.01, format="%.2f", key="deposito_valor"
        )
    with col_b:
        banco_escolhido = (
            st.selectbox("Banco de Destino", nomes_bancos, key="deposito_banco")
            if nomes_bancos else
            st.text_input("Banco de Destino (digite)", key="deposito_banco_text")
        )

    st.info("\n".join([
        "**Confirme os dados do depósito**",
        f"- **Data:** {pd.to_datetime(data_lanc).strftime('%d/%m/%Y')}",
        f"- **Valor:** {formatar_valor(valor or 0.0)}",
        f"- **Banco de destino:** {(banco_escolhido or '—')}",
        f"- **Origem do dinheiro:** Caixa 2 (primeiro do dia, depois saldo)",
    ]))

    confirmado = st.checkbox("Confirmo os dados acima", key="deposito_confirmar")
    submit = st.button(
        "💾 Registrar Depósito",
        use_container_width=True,
        key="deposito_salvar",
        disabled=not confirmado
    )

    return {
        "valor": float(valor or 0.0),
        "banco_escolhido": (banco_escolhido or "").strip(),
        "confirmado": bool(confirmado),
        "submit": bool(submit),
    }