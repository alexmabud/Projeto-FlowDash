# ===================== Actions: Página de Lançamentos =====================
"""
Consulta o SQLite e calcula os dados do resumo do dia.
Reproduz a mesma lógica do módulo original (sem alterações de regra).
"""

from __future__ import annotations

import pandas as pd
from shared.db import get_conn
from flowdash_pages.lancamentos.shared_ui import carregar_tabela



# ---- helpers internos (iguais ao original) --------------------------------
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
# ----------------------------------------------------------------------------

def carregar_resumo_dia(caminho_banco: str, data_lanc) -> dict:
    """
    Carrega totais e listas do dia selecionado, agregando:
    - total_vendas / total_saidas
    - snapshot de saldos (caixa / caixa 2)
    - transferências/depósitos do dia
    - mercadorias (compras/recebimentos do dia)
    - saldos_bancos acumulados até a data
    """
    # Entradas/Saídas (totais do dia)
    df_e = _padronizar_cols_fin(carregar_tabela("entrada", caminho_banco))
    df_s = _padronizar_cols_fin(carregar_tabela("saida", caminho_banco))

    total_vendas, total_saidas = 0.0, 0.0
    if not (df_e is None or df_e.empty) and {"data", "valor"}.issubset(df_e.columns):
        mask_e = df_e["data"].notna() & (df_e["data"].dt.date == data_lanc)
        total_vendas = float(df_e.loc[mask_e, "valor"].sum())
    if not (df_s is None or df_s.empty) and {"data", "valor"}.issubset(df_s.columns):
        mask_s = df_s["data"].notna() & (df_s["data"].dt.date == data_lanc)
        total_saidas = float(df_s.loc[mask_s, "valor"].sum())

    caixa_total = 0.0
    caixa2_total = 0.0
    transf_caixa2_total = 0.0
    depositos_list: list[tuple[str, float]] = []
    transf_bancos_list: list[tuple[str, str, float]] = []
    compras_list: list[tuple[str, str, float]] = []
    receb_list: list[tuple[str, str, float]] = []
    saldos_bancos: dict[str, float] = {}

    # Banco
    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # Snapshot saldos_caixas
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

        # Transferência p/ Caixa 2 (dia)
        transf_caixa2_total = cur.execute("""
            SELECT COALESCE(SUM(valor), 0)
              FROM movimentacoes_bancarias
             WHERE date(data)=?
               AND origem='transferencia_caixa'
        """, (str(data_lanc),)).fetchone()[0] or 0.0

        # Depósitos do dia
        depositos_list = cur.execute("""
            SELECT COALESCE(banco,''), COALESCE(valor,0.0)
              FROM movimentacoes_bancarias
             WHERE date(data)=? AND origem='deposito'
             ORDER BY rowid
        """, (str(data_lanc),)).fetchall()

        # Transferências banco→banco do dia (origem='transf_bancos')
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

        # Mercadorias do dia
        try:
            df_compras = pd.read_sql("SELECT * FROM mercadorias WHERE date(Data)=?", conn, params=(str(data_lanc),))
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
                "SELECT * FROM mercadorias WHERE Recebimento IS NOT NULL "
                "AND TRIM(Recebimento)<>'' AND date(Recebimento)=?",
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

        # Saldos bancos (acumulado <= data)
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

    return {
        "total_vendas": total_vendas,
        "total_saidas": total_saidas,
        "caixa_total": caixa_total,
        "caixa2_total": caixa2_total,
        "transf_caixa2_total": transf_caixa2_total,
        "depositos_list": depositos_list,
        "transf_bancos_list": transf_bancos_list,
        "compras_list": compras_list,
        "receb_list": receb_list,
        "saldos_bancos": saldos_bancos,
    }
