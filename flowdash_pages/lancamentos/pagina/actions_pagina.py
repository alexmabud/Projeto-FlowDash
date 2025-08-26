# ===================== Actions: Página de Lançamentos =====================
"""
Consulta o SQLite e calcula os dados do resumo do dia.
Reproduz a mesma lógica do módulo original, com salvaguardas
contra DUPLICIDADES (dedupe por trans_uid/id).
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
    - total_vendas: SOMA de entrada.Valor onde DATE(Data)=data_lanc  (venda do dia)
    - total_saidas: SOMA de saida.valor onde DATE(data)=data_lanc
    - caixa_total/caixa2_total: soma acumulada até a data (saldos_caixas)
    - saldos_bancos: soma acumulada por banco até a data (saldos_bancos)
    - listas do dia (depósitos, transferências, mercadorias)
    """
    total_vendas, total_saidas = 0.0, 0.0
    caixa_total = 0.0
    caixa2_total = 0.0
    transf_caixa2_total = 0.0
    depositos_list: list[tuple[str, float]] = []
    transf_bancos_list: list[tuple[str, str, float]] = []
    compras_list: list[tuple[str, str, float]] = []
    receb_list: list[tuple[str, str, float]] = []
    saldos_bancos: dict[str, float] = {}

    from shared.db import get_conn
    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # ======= VENDAS do dia (Data da VENDA, coluna Valor) =======
        total_vendas = float(cur.execute(
            """
            SELECT COALESCE(SUM(COALESCE(Valor,0)), 0.0)
              FROM entrada
             WHERE DATE(Data) = DATE(?)
            """,
            (str(data_lanc),),
        ).fetchone()[0] or 0.0)

        # ======= SAÍDAS do dia =======
        total_saidas = float(cur.execute(
            """
            SELECT COALESCE(SUM(COALESCE(valor,0)), 0.0)
              FROM saida
             WHERE DATE(data) = DATE(?)
            """,
            (str(data_lanc),),
        ).fetchone()[0] or 0.0)

        # ======= Caixas: SOMAR caixa_total e caixa2_total ATÉ a data =======
        try:
            import pandas as pd
            df_cx = pd.read_sql("SELECT data, caixa_total, caixa2_total FROM saldos_caixas", conn)
        except Exception:
            df_cx = pd.DataFrame()

        if not df_cx.empty:
            df_cx["data"] = pd.to_datetime(df_cx["data"], errors="coerce")
            df_cx = df_cx[df_cx["data"].dt.date <= data_lanc]

            if "caixa_total" in df_cx.columns:
                caixa_total = float(pd.to_numeric(df_cx["caixa_total"], errors="coerce").fillna(0.0).sum())
            if "caixa2_total" in df_cx.columns:
                caixa2_total = float(pd.to_numeric(df_cx["caixa2_total"], errors="coerce").fillna(0.0).sum())

        # ======= Transferência p/ Caixa 2 (dia) — dedupe por trans_uid/id =======
        transf_caixa2_total = cur.execute(
            """
            SELECT COALESCE(SUM(m.valor), 0.0)
              FROM movimentacoes_bancarias m
              JOIN (
                    SELECT MAX(id) AS id
                      FROM movimentacoes_bancarias
                     WHERE DATE(data)=DATE(?)
                       AND origem='transferencia_caixa'
                     GROUP BY COALESCE(trans_uid, CAST(id AS TEXT))
                   ) d ON d.id = m.id
            """,
            (str(data_lanc),),
        ).fetchone()[0] or 0.0

        # ======= Depósitos do dia — dedupe por trans_uid/id =======
        depositos_list = cur.execute(
            """
            SELECT m.banco, m.valor
              FROM movimentacoes_bancarias m
              JOIN (
                    SELECT MAX(id) AS id
                      FROM movimentacoes_bancarias
                     WHERE DATE(data)=DATE(?)
                       AND origem='deposito'
                     GROUP BY COALESCE(trans_uid, CAST(id AS TEXT))
                   ) d ON d.id = m.id
             ORDER BY m.id
            """,
            (str(data_lanc),),
        ).fetchall()

        # ======= Transferências banco→banco do dia — dedupe por trans_uid/id =======
        transf_bancos_raw = cur.execute(
            """
            SELECT m.banco, m.valor, m.observacao
              FROM movimentacoes_bancarias m
              JOIN (
                    SELECT MAX(id) AS id
                      FROM movimentacoes_bancarias
                     WHERE DATE(data)=DATE(?)
                       AND origem='transf_bancos'
                     GROUP BY COALESCE(trans_uid, CAST(id AS TEXT))
                   ) d ON d.id = m.id
             ORDER BY m.id
            """,
            (str(data_lanc),),
        ).fetchall()

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

        # ======= Mercadorias do dia =======
        compras_list, receb_list = [], []
        try:
            df_compras = pd.read_sql("SELECT * FROM mercadorias WHERE DATE(Data)=DATE(?)", conn, params=(str(data_lanc),))
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
                "AND TRIM(Recebimento)<>'' AND DATE(Recebimento)=DATE(?)",
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

        # ======= Saldos bancos (ACUMULADO <= data) — somar por coluna =======
        try:
            df_bk = pd.read_sql("SELECT * FROM saldos_bancos", conn)
        except Exception:
            df_bk = pd.DataFrame()

        if not df_bk.empty:
            date_col_name = next((c for c in df_bk.columns if c.lower() == "data"), None)
            if date_col_name:
                df_bk[date_col_name] = pd.to_datetime(df_bk[date_col_name], errors="coerce")
                df_bk = df_bk[df_bk[date_col_name].dt.date <= data_lanc]

            for c in df_bk.columns:
                if c.lower() == "data":
                    continue
                soma = pd.to_numeric(df_bk[c], errors="coerce").fillna(0.0).sum()
                saldos_bancos[str(c)] = float(soma)

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
