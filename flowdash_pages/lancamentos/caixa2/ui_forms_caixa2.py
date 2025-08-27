# flowdash_pages/lancamentos/caixa2/ui_forms_caixa2.py
"""
UI: Caixa 2

Resumo:
    Componente de formulário da página de Caixa 2, padronizado com os demais:
    - Sem contêiner/bordas extras
    - Campo: Valor da Transferência
    - Quadro de confirmação (Data + Valor)
    - Checkbox para habilitar o botão Salvar
    - Mensagem de instrução abaixo do botão

Entrada:
    - data_lanc (date | str 'YYYY-MM-DD'): data mostrada no cabeçalho e no resumo.

Saída:
    dict:
        valor (float): valor informado (>= 0.0)
        submit (bool): True apenas quando o usuário confirmou e clicou em Salvar
"""

from __future__ import annotations

from datetime import date, datetime
import streamlit as st

__all__ = ["render_form"]


def _fmt_brl(x: float | str | None) -> str:
    """Formata em BRL (ex.: 'R$ 1.234,56'), tolerando None/str."""
    try:
        n = float(x or 0.0)
    except Exception:
        n = 0.0
    s = f"{n:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"R$ {s}"


def _fmt_data_br(d: str | date) -> str:
    """Formata data para 'DD/MM/AAAA' a partir de date ou 'YYYY-MM-DD'."""
    if isinstance(d, date):
        return d.strftime("%d/%m/%Y")
    try:
        return datetime.strptime(str(d), "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        return str(d)


def render_form(data_lanc: str | date) -> dict:
    """Desenha o formulário de transferência para Caixa 2 e retorna os dados da submissão."""
    # Título com novo emoji
    st.markdown("#### ➡️ Lançar Transferência p/ Caixa 2")

    # Cabeçalho
    st.caption(f"Data do lançamento: **{_fmt_data_br(data_lanc)}**")

    # Input de valor
    valor = st.number_input(
        "Valor da Transferência",
        min_value=0.0,
        step=10.0,
        format="%.2f",
        key="caixa2_valor",
    )

    # Resumo para conferência (somente Data + Valor)
    st.info(
        f"**Confirme os dados da transferência**  \n"
        f"- Data: {_fmt_data_br(data_lanc)}  \n"
        f"- Valor: {_fmt_brl(valor)}"
    )

    # Confirmação + botão salvar
    confirmar: bool = st.checkbox(
        "Está tudo certo com os dados acima?", key="caixa2_confirma_widget"
    )
    submitted: bool = st.button(
        "💾 Salvar Transferência",
        use_container_width=True,
        disabled=not confirmar,
    )

    # Instrução abaixo do botão
    st.info("Confirme os dados para habilitar o botão de salvar.")

    return {
        "valor": float(valor or 0.0),
        "submit": bool(submitted and confirmar),
    }