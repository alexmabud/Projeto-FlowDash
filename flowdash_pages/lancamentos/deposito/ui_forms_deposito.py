# ===================== UI Forms: Depósito =====================
"""
Apenas UI – sem regra/SQL.
Campos:
- Banco de destino
- Valor
"""

from __future__ import annotations

from datetime import date
from typing import List, Dict, Callable
import streamlit as st

from utils.utils import formatar_valor


def render_form_deposito(
    data_lanc: date,
    nomes_bancos: List[str],
    invalidate_cb: Callable[[], None],
) -> Dict[str, object]:
    """
    Renderiza o formulário de Depósito (Caixa 2 → Banco). Somente interface.

    Args:
        data_lanc (date): Data do lançamento.
        nomes_bancos (list[str]): Lista de bancos cadastrados para seleção.
        invalidate_cb (Callable[[], None]): Callback disparado em on_change
            dos campos para invalidar a confirmação.

    Returns:
        dict[str, object]: Dicionário com:
            - "banco_destino" (str): Banco selecionado/digitado.
            - "valor" (float): Valor do depósito.
            - "confirmado" (bool): Se o usuário confirmou os dados.
    """
    # Título removido (o botão já exibe o nome da ação)
    st.caption(f"Data do lançamento: **{data_lanc}**")

    c1, c2 = st.columns(2)
    with c1:
        valor = st.number_input(
            "Valor do Depósito",
            min_value=0.0,
            step=0.01,
            format="%.2f",
            key="dep_valor",
            on_change=invalidate_cb,
        )
    with c2:
        if nomes_bancos:
            banco_destino = st.selectbox(
                "Banco de Destino",
                nomes_bancos,
                key="dep_banco_destino",
                on_change=invalidate_cb,
            )
        else:
            banco_destino = st.text_input(
                "Banco de Destino (digite)",
                key="dep_banco_destino_txt",
                on_change=invalidate_cb,
            )

    # Resumo (igual ao padrão da transferência)
    st.info(
        "\n".join(
            [
                "**Confirme os dados do depósito**",
                f"- **Data:** {data_lanc.strftime('%d/%m/%Y')}",
                f"- **Banco de destino:** {banco_destino or '—'}",
                f"- **Valor:** {formatar_valor(valor or 0.0)}",
                f"- **Origem:** Caixa 2",
            ]
        )
    )

    confirmado = st.checkbox(
        "Está tudo certo com os dados acima?", key="deposito_confirmado"
    )

    return {
        "banco_destino": (banco_destino or "").strip(),
        "valor": float(valor or 0.0),
        "confirmado": bool(confirmado),
    }
