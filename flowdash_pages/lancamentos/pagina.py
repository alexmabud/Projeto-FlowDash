import streamlit as st
from datetime import date
import pandas as pd

from .shared_ui import carregar_tabela, bloco_resumo_dia
from shared.db import get_conn
from utils.utils import formatar_valor
from .venda import render_venda
from .saida import render_saida
from .caixa2 import render_caixa2
from .deposito import render_deposito
from .transferencia_bancos import render_transferencia_bancaria
from .mercadorias import render_merc_compra, render_merc_recebimento


# ---------- Helpers locais para padronizar DataFrames ----------
def _padronizar_cols_fin(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renomeia colunas comuns para minúsculas e converte tipos:
    - Data/Data -> data (datetime)
    - Valor/valor -> valor (float)
    """
    if df is None or df.empty:
        return df

    # renomeia Data/Valor para data/valor
    ren = {}
    for c in df.columns:
        cl = c.lower()
        if cl == "data":
            ren[c] = "data"
        elif cl == "valor":
            ren[c] = "valor"
    df = df.rename(columns=ren)

    # tipos
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)

    return df
# ---------------------------------------------------------------


def pagina_lancamentos(caminho_banco: str):
    # Flash de sucesso
    if "msg_ok" in st.session_state:
        st.success(st.session_state.pop("msg_ok"))

    # Data do lançamento
    data_lanc = st.date_input("🗓️ Data do Lançamento", value=date.today(), key="data_lanc")
    st.markdown(f"## 🧾 Lançamentos do Dia — **{data_lanc}**")

    # ===== Preparar dados para o RESUMO =====
    df_e = _padronizar_cols_fin(carregar_tabela("entrada", caminho_banco))
    df_s = _padronizar_cols_fin(carregar_tabela("saida", caminho_banco))

    # Totais (resistentes a colunas ausentes)
    total_vendas = 0.0
    if not df_e.empty and "data" in df_e.columns and "valor" in df_e.columns:
        mask_e = df_e["data"].notna() & (df_e["data"].dt.date == data_lanc)
        total_vendas = float(df_e.loc[mask_e, "valor"].sum())

    total_saidas = 0.0
    if not df_s.empty and "data" in df_s.columns and "valor" in df_s.columns:
        mask_s = df_s["data"].notna() & (df_s["data"].dt.date == data_lanc)
        total_saidas = float(df_s.loc[mask_s, "valor"].sum())

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # Caixa e Caixa 2 (último saldo)
        row = cur.execute("""
            SELECT caixa_total, caixa2_total
              FROM saldos_caixas
          ORDER BY date(data) DESC, rowid DESC
             LIMIT 1
        """).fetchone()
        caixa_total = float(row[0]) if row and row[0] is not None else 0.0
        caixa2_total = float(row[1]) if row and row[1] is not None else 0.0

        # Totais do dia (movimentacoes_bancarias)
        depositos_total = cur.execute("""
            SELECT COALESCE(SUM(valor), 0)
              FROM movimentacoes_bancarias
             WHERE date(data)=? AND origem='deposito'
        """, (str(data_lanc),)).fetchone()[0] or 0.0

        transf_caixa2_total = cur.execute("""
            SELECT COALESCE(SUM(valor), 0)
              FROM movimentacoes_bancarias
             WHERE date(data)=?
               AND (origem='transferencia_caixa' OR observacao LIKE '%Caixa 2%')
        """, (str(data_lanc),)).fetchone()[0] or 0.0

        transf_bancos_total = cur.execute("""
            SELECT COALESCE(SUM(valor), 0)
              FROM movimentacoes_bancarias
             WHERE date(data)=? AND origem='transf_bancos'
        """, (str(data_lanc),)).fetchone()[0] or 0.0

        # Mercadorias (totais do dia)
        # Observação: em SQLite identificadores não-entre-aspas não diferenciam maiúsculas/minúsculas,
        # então date(Data) e date(data) funcionam igual; mantive "Data" conforme seu schema.
        compras_total = cur.execute("""
            SELECT COALESCE(SUM(Valor_Mercadoria), 0)
              FROM mercadorias
             WHERE date(Data)=?
        """, (str(data_lanc),)).fetchone()[0] or 0.0

        receb_total = cur.execute("""
            SELECT COALESCE(SUM(COALESCE(Valor_Recebido, Valor_Mercadoria)), 0)
              FROM mercadorias
             WHERE Recebimento IS NOT NULL
               AND TRIM(Recebimento) <> ''
               AND date(Recebimento)=?
        """, (str(data_lanc),)).fetchone()[0] or 0.0

        # Saldos dos bancos
        try:
            df_bancos_raw = pd.read_sql("SELECT * FROM saldos_bancos", conn)
            if not df_bancos_raw.empty:
                # Tenta formato "nome/saldo"
                nome_col = next((c for c in df_bancos_raw.columns if c.lower() in ("nome", "banco", "banco_nome", "instituicao")), None)
                saldo_col = next((c for c in df_bancos_raw.columns if c.lower() in ("saldo", "valor", "saldo_atual", "valor_atual")), None)
                if nome_col and saldo_col:
                    df_bancos = df_bancos_raw[[nome_col, saldo_col]].copy()
                    df_bancos.columns = ["Banco", "Saldo"]
                else:
                    # Formato "coluna por banco": pega a última linha por data e monta pares (Banco, Saldo)
                    cols = [c for c in df_bancos_raw.columns if c.lower() != "data"]
                    last = df_bancos_raw.tail(1)[cols] if not df_bancos_raw.empty else pd.DataFrame(columns=cols)
                    df_bancos = pd.DataFrame([
                        {"Banco": c, "Saldo": float(last.iloc[0][c] or 0.0) if not last.empty else 0.0}
                        for c in cols
                    ])
            else:
                df_bancos = pd.DataFrame(columns=["Banco", "Saldo"])
        except Exception:
            df_bancos = pd.DataFrame(columns=["Banco", "Saldo"])

    # ===== Cartão único do RESUMO =====
    linhas = [
        [("Vendas", formatar_valor(total_vendas)),
         ("Saídas", formatar_valor(total_saidas))],
        [("Caixa", formatar_valor(caixa_total)),
         ("Caixa 2", formatar_valor(caixa2_total))],
        ([(str(r["Banco"]), formatar_valor(r["Saldo"] or 0.0))
          for _, r in df_bancos.iterrows()] if not df_bancos.empty else []),
        [("→ Caixa 2", formatar_valor(transf_caixa2_total)),
         ("Depósitos", formatar_valor(depositos_total)),
         ("Transf. Bancos", formatar_valor(transf_bancos_total))],
        [("Compras Merc.", formatar_valor(compras_total)),
         ("Receb. Merc.", formatar_valor(receb_total))],
    ]
    bloco_resumo_dia(linhas, titulo="📆 Resumo do Dia")

    # ===== Ações =====
    st.markdown("### ➕ Ações")
    a1, a2 = st.columns(2)
    with a1:
        render_venda(caminho_banco, data_lanc)
    with a2:
        render_saida(caminho_banco, data_lanc)

    c1, c2, c3 = st.columns(3)
    with c1:
        render_caixa2(caminho_banco, data_lanc)
    with c2:
        render_deposito(caminho_banco, data_lanc)
    with c3:
        render_transferencia_bancaria(caminho_banco, data_lanc)

    st.markdown("---")

    st.markdown("### 📦 Mercadorias")
    render_merc_compra(caminho_banco, data_lanc)
    render_merc_recebimento(caminho_banco, data_lanc)