# ===================== UI: Cartões/Seções do Resumo =====================
"""
Componentes visuais (CSS + cartões) usados na página agregadora de Lançamentos.
Sem regras de negócio.
"""

from __future__ import annotations
import streamlit as st

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
.mini-table{ width:100%; border-collapse:collapse; }
.mini-table thead th{
  color:#cfd3df; font-size:.8rem; font-weight:700; letter-spacing:.2px;
  padding:5px 6px; border-bottom:1px solid var(--stroke); text-align:left;
}
.mini-table tbody td{ padding:5px 6px; border-bottom:1px solid var(--stroke); font-size:.88rem; vertical-align:top; }
.mini-table td.val{ color:var(--accent); font-weight:800; }
.small{ font-size:.8rem; color:var(--muted); }
</style>
"""

def _fmt_val(v) -> str:
    try:
        if v is None:
            v = 0.0
        v = float(v)
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"

def render_card_row(title: str, items: list[tuple[str, float | list[str] | None, bool]]):
    """
    Renderiza um cartão com título + UMA faixa com N colunas.

    Args:
        title: título do cartão.
        items: lista de tuplas (label, valor, number_always)
            - Se valor for list[str], mostra lista (verde).
            - Se float/None: number_always=True força exibir 0,00; senão mostra "Sem movimentações" para 0/None.
    """
    st.markdown(_CARD_TABLE_CSS, unsafe_allow_html=True)
    cells_html = []

    for label, value, number_always in items:
        if isinstance(value, (list, tuple)):
            if len(value) == 0:
                vhtml = '<div class="cell-empty">Sem movimentações</div>'
            else:
                linhas = ''.join(f'<div class="cell-item">{str(x)}</div>' for x in value)
                vhtml = f'<div class="cell-list">{linhas}</div>'
        else:
            try:
                is_zero = (value is None) or (float(value) == 0.0)
            except Exception:
                is_zero = True
            vhtml = f'<div class="cell-value">{_fmt_val(value or 0.0)}</div>' if (number_always or not is_zero) \
                    else '<div class="cell-empty">Sem movimentações</div>'

        cells_html.append(f'<div class="cell"><div class="cell-label">{label}</div>{vhtml}</div>')

    html = f"""
      <div class="section-card">
        <div class="section-header">{title}</div>
        <div class="section-row">{''.join(cells_html)}</div>
      </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def render_card_mercadorias(compras: list | None, recebimentos: list | None):
    """Cartão de Mercadorias com mini-tabelas (compras e recebimentos do dia)."""
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
