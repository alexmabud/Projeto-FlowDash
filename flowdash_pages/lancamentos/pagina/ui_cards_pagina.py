# ===================== UI: Cartões/Seções do Resumo =====================
"""
Componentes visuais (CSS + cartões) usados na página agregadora de Lançamentos.
Sem regras de negócio.

Notas:
    - `render_card_row(...)`: renderiza um cartão com uma linha e N células.
    - `render_card_mercadorias(...)`: exibe mini-tabelas para compras/recebimentos.
    - `get_transferencias_bancos_lista(...)`: formata as transferências banco→banco
      do dia (pareamento por token `TX=`; fallback em `id`).
"""

from __future__ import annotations

import streamlit as st
from .actions_pagina import listar_transferencias_bancos_do_dia

# NEW: suporte a DataFrame
try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None  # type: ignore


# ===================== CSS =====================
_CARD_TABLE_CSS = """
<style>
:root{
  --card:#13151A; --tile:#0F1115; --stroke:#20232B;
  --accent:#00FFA3; --muted:#A6AABB;
  --gap-card:1px; --gap-inside:10px;
}
.section-card{
  background:var(--card); border:0 solid var(--stroke); border-radius:18px;
  padding:var(--gap-inside) 14px; margin:var(--gap-card) 0;
  box-shadow:0 10px 24px rgba(0,0,0,.22), inset 0 1px 0 rgba(255,255,255,.02);
}
.section-header{ display:flex; align-items:center; gap:10px; font-weight:800; font-size:1.05rem; margin-bottom:8px; }
.section-row{ display:flex; border:1px solid var(--stroke); border-radius:12px; overflow:hidden; background:var(--tile); }
.cell{ flex:1 1 0; padding:8px 12px; display:flex; flex-direction:column; gap:6px; align-items:center; }
.cell + .cell{ border-left:1px solid var(--stroke); }
.cell-label{ color:#cfd3df; font-size:.85rem; font-weight:700; letter-spacing:.2px; }
.cell-value{ font-size:1.24rem; font-weight:900; color:var(--accent); text-shadow:0 0 16px rgba(0,255,163,.15); }
.cell-empty{ font-size:.82rem; font-weight:600; color:var(--muted); }
.cell-list{ display:flex; flex-direction:column; gap:4px; align-items:center; }
.cell-item{ color:var(--accent); font-weight:800; font-size:.98rem; }

.mini-table{ width:100%; border-collapse:collapse; table-layout:fixed; }
.mini-table thead th{
  color:#cfd3df; font-size:.8rem; font-weight:700; letter-spacing:.2px;
  padding:5px 6px; border-bottom:1px solid var(--stroke); text-align:left;
  white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
}
.mini-table tbody td{
  padding:5px 6px; border-bottom:1px solid var(--stroke); font-size:.88rem; vertical-align:top;
  white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
}
.mini-table td.val{ color:var(--accent); font-weight:800; }
.small{ font-size:.8rem; color:var(--muted); }
</style>
"""


# ===================== Helpers =====================
def _coerce_number(value) -> float:
    """Converte valores variados (float, int, str BR/EN) em float.

    Exemplos aceitos:
        - 1234.56
        - "1234.56"
        - "1.234,56"
        - "1,234.56"
        - "1234,56"

    Args:
        value: Valor numérico ou string.

    Returns:
        Número convertido para float (0.0 em caso de falha).
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    try:
        s = str(value).strip()
        if not s:
            return 0.0
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")       # "1.234,56" -> "1234.56"
        elif "," in s:
            s = s.replace(",", ".")                        # "1234,56"  -> "1234.56"
        elif s.count(",") == 1 and s.count(".") == 1 and s.find(",") < s.find("."):
            s = s.replace(",", "")                         # "1,234.56" -> "1234.56"
        return float(s)
    except Exception:
        return 0.0


def _fmt_val(v) -> str:
    """Formata número/str para moeda BR.

    Args:
        v: Valor a ser formatado.

    Returns:
        Valor no formato "R$ 1.234,56".
    """
    try:
        num = _coerce_number(v)
        return f"R$ {num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"


def _df_to_html_table(df) -> str:
    """Converte um DataFrame em HTML (classe 'mini-table').

    Args:
        df: pandas.DataFrame contendo os dados.

    Returns:
        Tabela HTML renderizada no padrão mini-table. Se pandas ausente, retorna mensagem.
    """
    if pd is None:
        return '<div class="cell-empty">Tabela indisponível (pandas ausente)</div>'
    if df is None or len(df) == 0:
        return '<div class="cell-empty">Sem movimentações</div>'

    _df = df.copy()
    cols = list(_df.columns)

    thead = "<thead><tr>" + "".join(f"<th>{c}</th>" for c in cols) + "</tr></thead>"

    rows_html = []
    for _, r in _df.iterrows():
        tds = []
        for c in cols:
            val = r.get(c, "")
            if str(c).strip().lower() == "valor":
                tds.append(f"<td class='val'>{_fmt_val(val)}</td>")
            else:
                tds.append(f"<td>{'' if val is None else str(val)}</td>")
        rows_html.append("<tr>" + "".join(tds) + "</tr>")
    tbody = "<tbody>" + "".join(rows_html) + "</tbody>"

    return f"<table class='mini-table'>{thead}{tbody}</table>"


# ===================== Cards =====================
def render_card_row(title: str, items: list[tuple[str, object, bool]]) -> None:
    """Renderiza um cartão com título e N colunas.

    Args:
        title: Título do cartão.
        items: Lista de tuplas (label, valor, number_always).
            - Se valor for list[str], mostra lista em verde.
            - Se for pandas.DataFrame, renderiza uma tabela HTML.
            - Se float/None: number_always=True força exibir 0,00;
              senão mostra "Sem movimentações".
            - Outros tipos: str via st.markdown.
    """
    st.markdown(_CARD_TABLE_CSS, unsafe_allow_html=True)
    items = list(items or [])
    cells_html = []

    is_df = (lambda v: (pd is not None and isinstance(v, pd.DataFrame)))

    for label, value, number_always in items:
        if is_df(value):
            vhtml = _df_to_html_table(value)
        elif isinstance(value, (list, tuple)):
            if len(value) == 0:
                vhtml = '<div class="cell-empty">Sem movimentações</div>'
            else:
                linhas = ''.join(f'<div class="cell-item">{str(x)}</div>' for x in value)
                vhtml = f'<div class="cell-list">{linhas}</div>'
        else:
            num = _coerce_number(value)
            vhtml = (
                f'<div class="cell-value">{_fmt_val(num)}</div>'
                if (number_always or num != 0.0)
                else '<div class="cell-empty">Sem movimentações</div>'
            )

        cells_html.append(f'<div class="cell"><div class="cell-label">{label}</div>{vhtml}</div>')

    html = f"""
      <div class="section-card">
        <div class="section-header">{title}</div>
        <div class="section-row">{''.join(cells_html)}</div>
      </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def render_card_mercadorias(compras: list | None, recebimentos: list | None) -> None:
    """Renderiza cartão de Mercadorias (compras e recebimentos do dia).

    Args:
        compras: Lista de tuplas (coleção, fornecedor, valor).
        recebimentos: Lista de tuplas (coleção, fornecedor, valor).
    """
    st.markdown(_CARD_TABLE_CSS, unsafe_allow_html=True)

    def _table(rows):
        if not rows:
            return '<div class="cell-empty">Sem movimentações</div>'
        body = []
        for colecao, fornecedor, valor in rows:
            body.append(
                f"<tr><td>{(colecao or '')}</td>"
                f"<td>{(fornecedor or '')}</td>"
                f"<td class='val'>{_fmt_val(valor or 0.0)}</td></tr>"
            )
        return ("<table class='mini-table'>"
                "<thead><tr><th>Coleção</th><th>Fornecedor</th><th>Valor</th></tr></thead>"
                f"<tbody>{''.join(body)}</tbody></table>")

    html = f"""
      <div class="section-card">
        <div class="section-header">📦 Mercadorias</div>
        <div class="section-row">
          <div class="cell">
            <div class="cell-label">Compras de Mercadorias</div>
            {_table(compras)}
          </div>
          <div class="cell">
            <div class="cell-label">Recebimento de Mercadorias</div>
            {_table(recebimentos)}
          </div>
        </div>
      </div>
    """
    st.markdown(html, unsafe_allow_html=True)


# ===================== Transferências =====================
def get_transferencias_bancos_lista(caminho_banco: str, data_lanc_str: str) -> list[str]:
    """Retorna linhas prontas para o card "Transferência entre bancos".

    Args:
        caminho_banco: Caminho para o SQLite.
        data_lanc_str: Data de referência (YYYY-MM-DD).

    Returns:
        Lista no formato ["R$ 5,00 • Bradesco → Inter", ...].
    """
    try:
        itens = listar_transferencias_bancos_do_dia(caminho_banco, data_lanc_str)
    except Exception:
        return []

    linhas: list[str] = []
    for it in itens:
        valor = _fmt_val(it.get("valor", 0.0))
        origem = str(it.get("origem") or "").strip()
        destino = str(it.get("destino") or "").strip()
        linhas.append(f"{valor} • {origem} → {destino}")
    return linhas


def render_transferencias_bancos(transf_bancos_list: list[tuple[str, str, float]]) -> None:
    """Renderiza resumo de transferências entre bancos.

    Mostra total do dia, quantidade de movimentações e tabela detalhada.

    Args:
        transf_bancos_list: Lista de tuplas (origem, destino, valor).
    """
    from utils.utils import formatar_moeda

    if not transf_bancos_list:
        st.caption("Sem movimentações.")
        return

    df = pd.DataFrame(transf_bancos_list, columns=["origem", "destino", "valor"])
    df["origem"] = df["origem"].replace("", "—")
    df["destino"] = df["destino"].replace("", "—")

    total = float(df["valor"].fillna(0).sum())
    qnt = int(len(df))

    c1, c2 = st.columns([1, 1])
    with c1:
        st.metric("Total transferido (dia)", formatar_moeda(total))
    with c2:
        st.metric("Movimentações", qnt)

    df_view = (
        df[["valor", "origem", "destino"]]
        .rename(columns={"valor": "Valor", "origem": "Origem", "destino": "Destino"})
    )
    df_view["Valor"] = df_view["Valor"].map(lambda v: formatar_moeda(float(v or 0)))
    st.dataframe(df_view, use_container_width=True, hide_index=True)
