# ===================== UI Forms: Venda =====================
"""
Componentes de UI para Venda. Apenas interface – sem regra/SQL.
Mantém os mesmos campos/fluxos do módulo original.
"""

from __future__ import annotations

import streamlit as st
import pandas as pd
from shared.db import get_conn

def _formas_equivalentes(forma: str):
    forma = (forma or "").upper()
    if forma == "LINK_PAGAMENTO":
        return ["LINK_PAGAMENTO", "LINK PAGAMENTO", "LINK-DE-PAGAMENTO", "LINK DE PAGAMENTO"]
    return [forma]

def render_form_venda(caminho_banco: str, data_lanc):
    """
    Desenha o formulário de venda e retorna os dados preenchidos (sem persistir).

    Returns:
        dict com dados para as ações: valor, forma, maquineta, bandeira, parcelas,
        modo_pix, banco_pix_direto, taxa_pix_direto.
    """
    st.markdown("#### 📋 Nova Venda")
    data_venda_str = pd.to_datetime(data_lanc).strftime("%d/%m/%Y")
    st.caption(f"Data do lançamento: **{data_venda_str}**")

    valor = st.number_input("Valor da Venda", min_value=0.0, step=0.01, key="venda_valor", format="%.2f")
    forma = st.selectbox("Forma de Pagamento", ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"], key="venda_forma")

    parcelas, bandeira, maquineta = 1, "", ""
    banco_pix_direto, taxa_pix_direto = None, 0.0

    # Carregar maquinetas (todas) — usado como fallback para UI
    try:
        with get_conn(caminho_banco) as conn:
            maq = pd.read_sql("SELECT DISTINCT maquineta FROM taxas_maquinas ORDER BY maquineta", conn)["maquineta"].tolist()
    except Exception:
        maq = []

    # PIX
    if forma == "PIX":
        modo_pix = st.radio("Como será o PIX?", ["Via maquineta", "Direto para banco"], horizontal=True, key="pix_modo")
        if modo_pix == "Via maquineta":
            with get_conn(caminho_banco) as conn:
                maq_pix = pd.read_sql(
                    "SELECT DISTINCT maquineta FROM taxas_maquinas WHERE UPPER(forma_pagamento)='PIX' ORDER BY maquineta",
                    conn
                )["maquineta"].tolist()
            if not maq_pix:
                st.warning("Nenhuma maquineta cadastrada para PIX. Cadastre em Cadastro → Taxas por Maquineta.")
                return None
            maquineta = st.selectbox("PSP/Maquineta do PIX", maq_pix, key="pix_maquineta")
        else:
            with get_conn(caminho_banco) as conn:
                try:
                    bancos = pd.read_sql("SELECT nome FROM bancos_cadastrados ORDER BY nome", conn)["nome"].tolist()
                except Exception:
                    bancos = []
            if not bancos:
                st.warning("Nenhum banco cadastrado. Cadastre em Cadastro → Bancos.")
                return None
            banco_pix_direto = st.selectbox("Banco que receberá o PIX", bancos, key="pix_banco")
            taxa_pix_direto  = st.number_input("Taxa do PIX direto (%)", min_value=0.0, step=0.01, value=0.0, format="%.2f", key="pix_taxa")

    # Cartões e Link de Pagamento
    elif forma in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"]:
        formas = _formas_equivalentes(forma)
        placeholders = ",".join(["?"] * len(formas))
        with get_conn(caminho_banco) as conn:
            maq_por_forma = pd.read_sql(
                f"""
                SELECT DISTINCT maquineta FROM taxas_maquinas
                WHERE UPPER(forma_pagamento) IN ({placeholders})
                ORDER BY maquineta
                """,
                conn,
                params=[f.upper() for f in formas]
            )["maquineta"].tolist()
        if not maq_por_forma:
            st.warning(f"Nenhuma maquineta cadastrada para {forma}. Cadastre em Cadastro → Taxas por Maquineta.")
            return None

        maquineta = st.selectbox("Maquineta", maq_por_forma, key="cartao_maquineta")

        with get_conn(caminho_banco) as conn:
            bandeiras = pd.read_sql(
                f"""
                SELECT DISTINCT bandeira FROM taxas_maquinas
                WHERE UPPER(forma_pagamento) IN ({placeholders}) AND maquineta=?
                ORDER BY bandeira
                """,
                conn,
                params=[f.upper() for f in formas] + [maquineta]
            )["bandeira"].tolist()
        if not bandeiras:
            st.warning(f"Nenhuma bandeira cadastrada para {forma} / {maquineta}. Cadastre em Cadastro → Taxas por Maquineta.")
            return None

        bandeira = st.selectbox("Bandeira", bandeiras, key="cartao_bandeira")

        with get_conn(caminho_banco) as conn:
            pars = pd.read_sql(
                f"""
                SELECT DISTINCT parcelas FROM taxas_maquinas
                WHERE UPPER(forma_pagamento) IN ({placeholders}) AND maquineta=? AND bandeira=?
                ORDER BY parcelas
                """,
                conn,
                params=[f.upper() for f in formas] + [maquineta, bandeira]
            )["parcelas"].tolist()
        if not pars:
            st.warning(f"Nenhuma parcela cadastrada para {forma} / {maquineta} / {bandeira}. Cadastre em Cadastro → Taxas por Maquineta.")
            return None

        parcelas = st.selectbox("Parcelas", pars, key="cartao_parcelas")
        st.caption(f"Parcelas: **{int(parcelas or 1)}×**")

    else:
        st.caption("🧾 Venda em dinheiro será registrada no **Caixa**.")

    # Resumo
    linhas_md = [
        "**Confirme os dados da venda**",
        f"- **Data:** {data_venda_str}",
        f"- **Valor:** R$ {float(valor or 0.0):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        f"- **Forma de pagamento:** {forma}",
    ]
    if forma in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"]:
        linhas_md += [
            f"- **Maquineta:** {maquineta or '—'}",
            f"- **Bandeira:** {bandeira or '—'}",
            f"- **Parcelas:** {int(parcelas or 1)}x",
        ]
    elif forma == "PIX":
        if st.session_state.get("pix_modo") == "Via maquineta":
            linhas_md += [f"- **PIX via maquineta:** {maquineta or '—'}"]
        else:
            linhas_md += [
                f"- **PIX direto ao banco:** {banco_pix_direto or '—'}",
                f"- **Taxa informada (%):** {float(taxa_pix_direto or 0.0):.2f}"
            ]
    st.info("\n".join(linhas_md))

    confirmado = st.checkbox("Confirmo os dados acima", key="venda_confirmar")

    return {
        "valor": float(valor or 0.0),
        "forma": (forma or "").strip(),
        "parcelas": int(parcelas or 1),
        "bandeira": (bandeira or "").strip(),
        "maquineta": (maquineta or "").strip(),
        "modo_pix": st.session_state.get("pix_modo") if forma == "PIX" else None,
        "banco_pix_direto": (banco_pix_direto or "").strip() if banco_pix_direto else None,
        "taxa_pix_direto": float(taxa_pix_direto or 0.0),
        "confirmado": bool(confirmado),
    }