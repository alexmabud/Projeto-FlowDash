import streamlit as st
import sqlite3
import pandas as pd
from datetime import date
from utils.utils import formatar_valor

# ------------------ helpers ------------------
def _get_saldo_caixas(caminho_banco: str, data_ref: str):
    """Retorna (caixa, caixa_2) cadastrados em saldos_caixas para a data."""
    with sqlite3.connect(caminho_banco) as conn:
        row = conn.execute(
            "SELECT caixa, caixa_2 FROM saldos_caixas WHERE data = ? LIMIT 1",
            (data_ref,)
        ).fetchone()
    if row:
        return float(row[0] or 0.0), float(row[1] or 0.0)
    return 0.0, 0.0

def _get_movimentos_caixa(caminho_banco: str, data_ref: str):
    """Movimentações do dia (Caixa / Caixa 2) em movimentacoes_bancarias."""
    like_pat = f"{data_ref}%"
    with sqlite3.connect(caminho_banco) as conn:
        df = pd.read_sql(
            """
            SELECT id, data, banco, tipo, origem, valor, observacao,
                   referencia_tabela, referencia_id
            FROM movimentacoes_bancarias
            WHERE data LIKE ?
              AND banco IN ('Caixa','Caixa 2')
            ORDER BY id
            """,
            conn,
            params=(like_pat,)
        )
    # normaliza tipos
    if not df.empty:
        df["tipo"] = df["tipo"].str.lower().str.strip()
        df["origem"] = df["origem"].astype(str).str.strip()
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
    return df

# ------------------ página ------------------
def pagina_fechamento_caixa(caminho_banco: str):
    st.subheader("🧾 Fechamento de Caixa — v1")

    data_sel = st.date_input("📅 Data do fechamento", value=date.today())
    data_ref = str(data_sel)

    # Saldos cadastrados (operacionais)
    caixa, caixa2 = _get_saldo_caixas(caminho_banco, data_ref)

    # Movimentações do dia (Caixa/Caixa 2)
    df_mov = _get_movimentos_caixa(caminho_banco, data_ref)

    # Totais do dia
    if not df_mov.empty:
        entradas_total = df_mov.loc[df_mov["tipo"] == "entrada", "valor"].sum()
        saidas_total = df_mov.loc[df_mov["tipo"] == "saida", "valor"].sum()

        # Correções de caixa no dia (entrada/saída)
        df_corr = df_mov[df_mov["origem"] == "correcao_caixa"]
        corr_ent = df_corr.loc[df_corr["tipo"] == "entrada", "valor"].sum() if not df_corr.empty else 0.0
        corr_sai = df_corr.loc[df_corr["tipo"] == "saida", "valor"].sum() if not df_corr.empty else 0.0
        corr_liq = corr_ent - corr_sai
    else:
        entradas_total = saidas_total = corr_ent = corr_sai = corr_liq = 0.0

    # UI — Resumo
    st.markdown("#### 📌 Resumo do Dia")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Entradas do dia", formatar_valor(entradas_total))
    with col2:
        st.metric("Saídas do dia", formatar_valor(saidas_total))
    with col3:
        st.metric("Correções (líquido)", formatar_valor(corr_liq))
    with col4:
        st.metric("Mov. líquido do dia", formatar_valor(entradas_total - saidas_total))

    st.markdown("#### 🧮 Saldos Cadastrados (saldos_caixas)")
    colA, colB = st.columns(2)
    with colA:
        st.info(f"**Caixa (loja)** em {pd.to_datetime(data_ref).strftime('%d/%m/%Y')}: {formatar_valor(caixa)}")
    with colB:
        st.info(f"**Caixa 2 (casa)** em {pd.to_datetime(data_ref).strftime('%d/%m/%Y')}: {formatar_valor(caixa2)}")

    st.caption("Obs.: este v1 não recalcula abertura/fechamento; mostra saldos operacionais e os movimentos do dia. No v2 incluiremos abertura (dia anterior) e reconciliação completa.")

    # Detalhe por origem/banco
    st.markdown("---")
    st.markdown("### 🔎 Detalhe de Movimentações (Caixa / Caixa 2)")
    if df_mov.empty:
        st.info("Sem movimentações para este dia em Caixa/Caixa 2.")
        return

    # Tabela resumo por banco e tipo
    df_res = (
        df_mov
        .groupby(["banco", "tipo"], as_index=False)["valor"]
        .sum()
        .pivot(index="banco", columns="tipo", values="valor")
        .fillna(0.0)
        .reset_index()
    )
    # formata
    for c in [c for c in df_res.columns if c != "banco"]:
        df_res[c] = df_res[c].apply(formatar_valor)
    st.markdown("**Por banco e tipo:**")
    st.dataframe(df_res, use_container_width=True, hide_index=True)

    # Tabela resumo por origem
    df_origem = (
        df_mov
        .groupby(["origem", "tipo"], as_index=False)["valor"]
        .sum()
        .pivot(index="origem", columns="tipo", values="valor")
        .fillna(0.0)
        .reset_index()
    )
    for c in [c for c in df_origem.columns if c != "origem"]:
        df_origem[c] = df_origem[c].apply(formatar_valor)
    st.markdown("**Por origem:**")
    st.dataframe(df_origem, use_container_width=True, hide_index=True)

    # Detalhado (linhas)
    with st.expander("🗂️ Ver lançamentos (linhas)"):
        df_show = df_mov.copy()
        # formata
        df_show["data"] = pd.to_datetime(df_show["data"], errors="coerce")
        df_show["Data"] = df_show["data"].dt.strftime("%d/%m/%Y %H:%M").fillna("")
        df_show["Valor (R$)"] = df_show["valor"].apply(formatar_valor)
        df_show = df_show.rename(columns={
            "banco": "Banco", "tipo": "Tipo", "origem": "Origem",
            "observacao": "Observação", "referencia_tabela": "Ref. Tabela",
            "referencia_id": "Ref. ID"
        })
        cols = ["id", "Data", "Banco", "Tipo", "Origem", "Valor (R$)", "Observação", "Ref. Tabela", "Ref. ID"]
        cols = [c for c in cols if c in df_show.columns]
        st.dataframe(df_show[cols], use_container_width=True, hide_index=True)