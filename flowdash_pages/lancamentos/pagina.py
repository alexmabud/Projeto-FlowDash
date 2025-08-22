"""
Módulo Página
=============

Este módulo organiza a **estrutura de navegação** e renderização das páginas 
no FlowDash. Ele atua como um ponto central para carregar os módulos de 
lançamentos, cadastros, relatórios e dashboards dentro da interface Streamlit.

Funcionalidades principais
--------------------------
- Controle de **menu lateral** e seleção de páginas.
- Carregamento dinâmico de subpáginas (Entradas, Saídas, Transferências, 
  Mercadorias, Empréstimos, etc.).
- Integração com controle de **perfis de usuário** (Administrador, Gerente, 
  Vendedor), restringindo o acesso a determinadas seções.
- Ponto único para configurar o **layout padrão** da aplicação 
  (título, ícones, estilo, cabeçalho e rodapé).
- Suporte a session state para persistir a navegação do usuário.

Detalhes técnicos
-----------------
- Implementado em Streamlit.
- Utiliza componentes reutilizáveis definidos no módulo `shared_ui`.
- Repositórios e serviços são injetados conforme a página acessada.
- Mantém coesão entre o fluxo de navegação e os módulos de lançamentos e cadastros.

Dependências
------------
- streamlit
- pandas
- datetime
- shared_ui (componentes visuais)
- services.* (LedgerService e outros)
- repositories.* (bancos, categorias, cartões, movimentações)
- flowdash_pages.* (módulos específicos de páginas)

"""

import streamlit as st
from datetime import date
import pandas as pd

from .shared_ui import carregar_tabela
from shared.db import get_conn
from .venda import render_venda
from .saida import render_saida
from .caixa2 import render_caixa2
from .deposito import render_deposito
from .transferencia_bancos import render_transferencia_bancaria
from .mercadorias import render_merc_compra, render_merc_recebimento


# ========= CSS + helpers (cartão com "faixa" e divisórias internas) =========
_CARD_TABLE_CSS = """
<style>
:root{
  --card:#13151A; --tile:#0F1115; --stroke:#20232B;
  --accent:#00FFA3; --muted:#A6AABB;
  --gap-card:1px;     /* espaço ENTRE blocos */
  --gap-inside:10px;  /* espaço interno do cartão */
}
.section-card{
  background:var(--card);
  border:0px solid var(--stroke);
  border-radius:18px;
  padding:var(--gap-inside) 14px;
  margin:var(--gap-card) 0;
  box-shadow:0 10px 24px rgba(0,0,0,.22), inset 0 1px 0 rgba(255,255,255,.02);
}
.section-header{
  display:flex; align-items:center; gap:10px;
  font-weight:800; font-size:1.05rem; margin-bottom:8px;
}
.section-row{
  display:flex; border:1px solid var(--stroke); border-radius:12px;
  overflow:hidden; background:var(--tile);
}
.cell{
  flex:1 1 0; padding:8px 12px;
  display:flex; flex-direction:column; gap:6px; align-items:center;
}
.cell + .cell{ border-left:1px solid var(--stroke); }
.cell-label{ color:#cfd3df; font-size:.85rem; font-weight:700; letter-spacing:.2px; }
.cell-value{
  font-size:1.24rem; font-weight:900; color:var(--accent);
  text-shadow:0 0 16px rgba(0,255,163,.15);
}
.cell-empty{ font-size:.82rem; font-weight:600; color:var(--muted); }

/* lista de itens (linhas) dentro de uma célula, mantendo o verde */
.cell-list{ display:flex; flex-direction:column; gap:4px; align-items:center; }
.cell-item{ color:var(--accent); font-weight:800; font-size:.98rem; }

/* mini-tabela para Mercadorias (compacta) */
.mini-table{ width:100%; border-collapse:collapse; }
.mini-table thead th{
  color:#cfd3df; font-size:.8rem; font-weight:700; letter-spacing:.2px;
  padding:5px 6px; border-bottom:1px solid var(--stroke); text-align:left;
}
.mini-table tbody td{
  padding:5px 6px; border-bottom:1px solid var(--stroke); font-size:.88rem; vertical-align:top;
}
.mini-table td.val{ color:var(--accent); font-weight:800; }

.small{ font-size:.8rem; color:var(--muted); }
</style>
"""

def _fmt_val(v):
    try:
        if v is None:
            v = 0.0
        v = float(v)
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"

def _card_row(title: str, items: list):
    """
    Renderiza um cartão com título + UMA faixa com N colunas.
    items: lista de tuplas (label:str, valor:float|list[str]|None, number_always:bool)
      - number_always=True: sempre mostra número (até se 0,00)
      - False: mostra 'Sem movimentações' quando 0/None (para números)
      - Se valor for list[str], renderiza cada linha dentro da célula (verde).
    """
    st.markdown(_CARD_TABLE_CSS, unsafe_allow_html=True)

    cells = []
    for label, value, number_always in items:
        # lista de linhas (detalhes)
        if isinstance(value, (list, tuple)):
            if len(value) == 0:
                vhtml = '<div class="cell-empty">Sem movimentações</div>'
            else:
                linhas = ''.join(f'<div class="cell-item">{str(x)}</div>' for x in value)
                vhtml = f'<div class="cell-list">{linhas}</div>'
        else:
            # numérico
            try:
                is_zero = (value is None) or (float(value) == 0.0)
            except Exception:
                is_zero = True

            if number_always or not is_zero:
                vhtml = f'<div class="cell-value">{_fmt_val(value or 0.0)}</div>'
            else:
                vhtml = '<div class="cell-empty">Sem movimentações</div>'

        cells.append(
            f'<div class="cell"><div class="cell-label">{label}</div>{vhtml}</div>'
        )

    html = f'''
      <div class="section-card">
        <div class="section-header">{title}</div>
        <div class="section-row">{''.join(cells)}</div>
      </div>
    '''
    st.markdown(html, unsafe_allow_html=True)

def _card_row_mercadorias(compras: list | None, recebimentos: list | None):
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
        return (
            "<table class='mini-table'>"
            "<thead><tr><th>Coleção</th><th>Fornecedor</th><th>Valor</th></tr></thead>"
            f"<tbody>{''.join(body)}</tbody></table>"
        )

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


# ---------- Helpers ----------
def _padronizar_cols_fin(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    ren = {}
    for c in df.columns:
        cl = c.lower()
        if cl == "data":
            ren[c] = "data"
        elif cl == "valor":
            ren[c] = "valor"
    df = df.rename(columns=ren)
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce", dayfirst=True)
    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
    return df

def _coerce_date_col(df: pd.DataFrame, guess_names=("data","Data")) -> str | None:
    if df is None or df.empty:
        return None
    cols_low = {c.lower(): c for c in df.columns}
    for k in guess_names:
        real = cols_low.get(k.lower())
        if real:
            try:
                df[real] = pd.to_datetime(df[real], errors="coerce", dayfirst=True)
                return real
            except Exception:
                return real
    return None


# =========================
# PÁGINA
# =========================
def pagina_lancamentos(caminho_banco: str):
    if "msg_ok" in st.session_state:
        st.success(st.session_state.pop("msg_ok"))

    data_lanc = st.date_input("🗓️ Data do Lançamento", value=date.today(), key="data_lanc")
    st.markdown(f"## 🧾 Lançamentos do Dia — **{data_lanc}**")

    # ===== Resumo (entradas/saídas do dia) =====
    df_e = _padronizar_cols_fin(carregar_tabela("entrada", caminho_banco))
    df_s = _padronizar_cols_fin(carregar_tabela("saida", caminho_banco))

    total_vendas, total_saidas = 0.0, 0.0
    if not df_e.empty and {"data", "valor"}.issubset(df_e.columns):
        mask_e = df_e["data"].notna() & (df_e["data"].dt.date == data_lanc)
        total_vendas = float(df_e.loc[mask_e, "valor"].sum())
    if not df_s.empty and {"data", "valor"}.issubset(df_s.columns):
        mask_s = df_s["data"].notna() & (df_s["data"].dt.date == data_lanc)
        total_saidas = float(df_s.loc[mask_s, "valor"].sum())

    # ===== Consultas / Leitura para Saldos =====
    caixa_total = 0.0
    caixa2_total = 0.0
    transf_caixa2_total = 0.0
    depositos_list = []
    transf_bancos_list = []
    compras_list = []
    receb_list = []
    saldos_bancos = {}

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # ---- CAIXA / CAIXA 2 — Snapshot do dia (sem somar linhas)
        try:
            df_caixas = pd.read_sql("SELECT * FROM saldos_caixas", conn)
            if not df_caixas.empty:
                cols_low = {c.lower(): c for c in df_caixas.columns}
                date_col = _coerce_date_col(df_caixas, guess_names=("data","Data"))
                c_caixa  = cols_low.get("caixa_total")  or "caixa_total"
                c_caixa2 = cols_low.get("caixa2_total") or "caixa2_total"

                if c_caixa in df_caixas.columns:
                    df_caixas[c_caixa] = pd.to_numeric(df_caixas[c_caixa], errors="coerce").fillna(0.0)
                if c_caixa2 in df_caixas.columns:
                    df_caixas[c_caixa2] = pd.to_numeric(df_caixas[c_caixa2], errors="coerce").fillna(0.0)

                snap = None
                if date_col:
                    same_day = df_caixas[df_caixas[date_col].dt.date == data_lanc]
                    if not same_day.empty:
                        snap = same_day.sort_values(date_col).tail(1)
                    else:
                        prev = df_caixas[df_caixas[date_col].dt.date <= data_lanc]
                        if not prev.empty:
                            snap = prev.sort_values(date_col).tail(1)
                if snap is None or snap.empty:
                    snap = df_caixas.tail(1)

                if snap is not None and not snap.empty:
                    caixa_total  = float(snap.iloc[0].get(c_caixa, 0.0)  or 0.0)
                    caixa2_total = float(snap.iloc[0].get(c_caixa2, 0.0) or 0.0)
        except Exception:
            pass

        # ---- Transferências/Depósitos do dia (para os cartões do dia)
        transf_caixa2_total = cur.execute("""
            SELECT COALESCE(SUM(valor), 0)
              FROM movimentacoes_bancarias
             WHERE date(data)=?
               AND origem='transferencia_caixa'
        """, (str(data_lanc),)).fetchone()[0] or 0.0

        # lista de depósitos do dia
        depositos_list = cur.execute("""
            SELECT COALESCE(banco,''), COALESCE(valor,0.0)
              FROM movimentacoes_bancarias
             WHERE date(data)=? AND origem='deposito'
             ORDER BY rowid
        """, (str(data_lanc),)).fetchall()

        # transferências banco→banco do dia
        transf_bancos_raw = cur.execute("""
            SELECT COALESCE(banco,''), COALESCE(valor,0.0), COALESCE(observacao,'')
              FROM movimentacoes_bancarias
             WHERE date(data)=? AND origem='transf_bancos'
             ORDER BY rowid
        """, (str(data_lanc),)).fetchall()

        transf_bancos_list = []
        for banco, valor, obs in transf_bancos_raw:
            de_val, para_val = "", ""
            txt = (obs or "").strip()
            if "->" in txt:
                p = [t.strip() for t in txt.split("->", 1)]
                de_val = p[0] if p else ""
                para_val = p[1] if len(p) > 1 else ""
            elif "para" in txt.lower():
                try:
                    pre, pos = txt.split("para", 1)
                    de_val = pre.replace("de","").strip()
                    para_val = pos.strip()
                except Exception:
                    pass
            if not de_val and not para_val:
                para_val = banco or ""
            transf_bancos_list.append((de_val, para_val, float(valor or 0.0)))

        # ---- Mercadorias — listas (apenas do dia selecionado)
        try:
            df_compras = pd.read_sql(
                "SELECT * FROM mercadorias WHERE date(Data)=?",
                conn, params=(str(data_lanc),)
            )
            if not df_compras.empty:
                cols = {c.lower(): c for c in df_compras.columns}
                col_col = cols.get("colecao") or cols.get("coleção")
                col_forn = cols.get("fornecedor")
                col_val = cols.get("valor_mercadoria")
                for _, r in df_compras.iterrows():
                    compras_list.append((
                        str(r.get(col_col, "") if col_col else ""),
                        str(r.get(col_forn, "") if col_forn else ""),
                        float(r.get(col_val, 0) or 0.0) if col_val else 0.0
                    ))
        except Exception:
            pass

        try:
            df_receb = pd.read_sql(
                "SELECT * FROM mercadorias "
                "WHERE Recebimento IS NOT NULL AND TRIM(Recebimento)<>'' AND date(Recebimento)=?",
                conn, params=(str(data_lanc),)
            )
            if not df_receb.empty:
                cols = {c.lower(): c for c in df_receb.columns}
                col_col = cols.get("colecao") or cols.get("coleção")
                col_forn = cols.get("fornecedor")
                col_vr = cols.get("valor_recebido")
                col_vm = cols.get("valor_mercadoria")
                for _, r in df_receb.iterrows():
                    valor = float(r.get(col_vr)) if (col_vr and pd.notna(r.get(col_vr))) else float(r.get(col_vm, 0) or 0.0)
                    receb_list.append((
                        str(r.get(col_col, "") if col_col else ""),
                        str(r.get(col_forn, "") if col_forn else ""),
                        valor
                    ))
        except Exception:
            pass

        # ---------------- SALDOS DOS BANCOS (ACUMULADO <= data_lanc) ----------------
        try:
            df_bancos_raw = pd.read_sql("SELECT * FROM saldos_bancos", conn)
            if not df_bancos_raw.empty:
                df_bancos = df_bancos_raw.copy()
                date_col = _coerce_date_col(df_bancos, guess_names=("data","Data"))

                nome_col  = next((c for c in df_bancos.columns if c.lower() in ("nome","banco","banco_nome","instituicao")), None)
                valor_col = next((c for c in df_bancos.columns if c.lower() in ("saldo","valor","saldo_atual","valor_atual")), None)

                if date_col:
                    df_bancos = df_bancos[df_bancos[date_col].dt.date <= data_lanc]

                if not df_bancos.empty and nome_col and valor_col:
                    df_bancos[valor_col] = pd.to_numeric(df_bancos[valor_col], errors="coerce").fillna(0.0)
                    grouped = df_bancos.groupby(nome_col, dropna=True, as_index=False)[valor_col].sum()
                    saldos_bancos = { str(r[nome_col]): float(r[valor_col] or 0.0) for _, r in grouped.iterrows() }
                else:
                    saldos_bancos = {}
                    cols_sum = [c for c in df_bancos.columns if c != date_col] if date_col else list(df_bancos.columns)
                    for c in cols_sum:
                        try:
                            sal = pd.to_numeric(df_bancos[c], errors="coerce").fillna(0.0).sum()
                            if pd.notna(sal):
                                saldos_bancos[str(c)] = float(sal)
                        except Exception:
                            pass
        except Exception:
            saldos_bancos = {}
        # -----------------------------------------------------------------------------


    # ====================== CARTÕES (faixa com divisórias) ======================

    # 1) Resumo do Dia — 2 colunas
    _card_row("📊 Resumo do Dia", [
        ("Vendas", total_vendas, True),
        ("Saídas", total_saidas, True),
    ])

    # 2) Saldos — 5 colunas
    nb = { (k or "").strip().lower(): float(v or 0.0) for k, v in (saldos_bancos or {}).items() }
    inter    = nb.get("inter", 0.0)
    infinite = nb.get("infinitepay", nb.get("infinitiepay", nb.get("infinite pay", 0.0)))
    bradesco = nb.get("bradesco", 0.0)
    _card_row("💵 Saldos", [
        ("Caixa",       caixa_total,  True),
        ("Caixa 2",     caixa2_total, True),
        ("Inter",       inter,        True),
        ("InfinitePay", infinite,     True),
        ("Bradesco",    bradesco,     True),
    ])

    # 3) Transferências — detalhado
    dep_lin = [f"{_fmt_val(v)} → {(b or '—')}" for (b, v) in (depositos_list or [])]
    trf_lin = []
    for de, para, v in (transf_bancos_list or []):
        de_txt = (de or "").strip()
        trf_lin.append(f"{_fmt_val(v)} {'%s ' % de_txt if de_txt else ''}→ {(para or '—')}")
    _card_row("🔁 Transferências", [
        ("P/ Caixa 2",                 transf_caixa2_total, False),   # mantém total
        ("Depósito Bancário",          dep_lin,             False),   # lista
        ("Transferência entre bancos", trf_lin,             False),   # lista
    ])

    # 4) Mercadorias
    _card_row_mercadorias(compras_list, receb_list)

    # ====================== AÇÕES ======================
    st.markdown("### ➕ Ações")
    a1, a2 = st.columns(2)
    with a1: render_venda(caminho_banco, data_lanc)
    with a2: render_saida(caminho_banco, data_lanc)
    c1, c2, c3 = st.columns(3)
    with c1: render_caixa2(caminho_banco, data_lanc)
    with c2: render_deposito(caminho_banco, data_lanc)
    with c3: render_transferencia_bancaria(caminho_banco, data_lanc)

    st.markdown("---")
    st.markdown("### 📦 Mercadorias — Lançamentos")
    render_merc_compra(caminho_banco, data_lanc)
    render_merc_recebimento(caminho_banco, data_lanc)