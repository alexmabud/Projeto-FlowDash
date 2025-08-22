# ===================== UI Forms: Transferência =====================
"""
Componentes de UI para Transferência Banco → Banco. Apenas interface – sem regra/SQL.
"""

import streamlit as st
import pandas as pd
from utils.utils import formatar_valor

def render_form(data_lanc, nomes_bancos: list[str]) -> dict:
    """
    Desenha o formulário de transferência.

    Args:
        data_lanc: data do lançamento.
        nomes_bancos: lista de bancos cadastrados.

    Returns:
        dict: {"banco_origem": str, "banco_destino": str, "valor": float, "confirmado": bool, "submit": bool}
    """
    st.markdown("#### 🔁 Transferência Banco → Banco")
    st.caption(f"Data do lançamento: **{pd.to_datetime(data_lanc).strftime('%d/%m/%Y')}**")

    col_a, col_b = st.columns(2)
    with col_a:
        banco_origem = (
            st.selectbox("Banco de Origem", nomes_bancos, key="transf_banco_origem")
            if nomes_bancos else
            st.text_input("Banco de Origem (digite)", key="transf_banco_origem_text")
        )
    with col_b:
        banco_destino = (
            st.selectbox("Banco de Destino", nomes_bancos, key="transf_banco_destino")
            if nomes_bancos else
            st.text_input("Banco de Destino (digite)", key="transf_banco_destino_text")
        )

    valor = st.number_input("Valor da Transferência", min_value=0.0, step=0.01, format="%.2f", key="transf_bancos_valor")

    st.info("\n".join([
        "**Confirme os dados da transferência**",
        f"- **Data:** {pd.to_datetime(data_lanc).strftime('%d/%m/%Y')}",
        f"- **Valor:** {formatar_valor(valor or 0.0)}",
        f"- **Origem:** {(banco_origem or '—')}",
        f"- **Destino:** {(banco_destino or '—')}",
        "- Serão criadas 2 linhas com o MESMO referencia_id (id da SAÍDA).",
    ]))

    confirmado = st.checkbox("Confirmo os dados acima", key="transf_bancos_confirmar")
    submit = st.button("💾 Registrar Transferência", use_container_width=True, key="transf_bancos_salvar", disabled=not confirmado)

    return {
        "banco_origem": (banco_origem or "").strip(),
        "banco_destino": (banco_destino or "").strip(),
        "valor": float(valor or 0.0),
        "confirmado": bool(confirmado),
        "submit": bool(submit),
    }