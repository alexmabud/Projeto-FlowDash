# ===================== UI Forms: Caixa 2 =====================
"""
Componentes de UI (inputs, checkbox e botão) para Caixa 2.

Somente UI – sem regra de negócio. A lógica de gravação fica em `actions.py`.
"""

import streamlit as st

def render_form() -> dict:
    """
    Desenha o formulário de transferência para o Caixa 2.

    Returns:
        dict: { "valor": float, "confirmado": bool, "submit": bool }
    """
    st.markdown("#### 💸 Transferência para Caixa 2")

    valor = st.number_input(
        "Valor a Transferir",
        min_value=0.0,
        step=0.01,
        key="caixa2_valor",
        format="%.2f"
    )
    confirmado = st.checkbox("Confirmo a transferência", key="caixa2_confirma")
    submit = st.button(
        "💾 Confirmar Transferência",
        use_container_width=True,
        key="caixa2_salvar",
        disabled=not confirmado
    )
    return {"valor": float(valor or 0.0), "confirmado": bool(confirmado), "submit": bool(submit)}