# ===================== UI Forms: Venda =====================
"""
Componentes de UI para Venda. Apenas interface – sem regra/SQL.
Mantém os mesmos campos/fluxos do módulo original.
"""

from __future__ import annotations

import streamlit as st
import pandas as pd
from typing import Optional, List

from shared.db import get_conn
from utils.utils import formatar_moeda   # [unifica moeda]
from .state_venda import invalidate_confirm


def _formas_equivalentes(forma: str) -> List[str]:
    forma = (forma or "").upper()
    if forma == "LINK_PAGAMENTO":
        return ["LINK_PAGAMENTO", "LINK PAGAMENTO", "LINK-DE-PAGAMENTO", "LINK DE PAGAMENTO"]
    return [forma]


def render_form_venda(caminho_banco: str, data_lanc):
    """
    Desenha o formulário de venda e retorna os dados preenchidos (sem persistir).

    Returns:
        dict com dados para as ações: valor, forma, maquineta, bandeira, parcelas,
        modo_pix, banco_pix_direto, taxa_pix_direto, confirmado.
        Retorna None quando falta cadastro necessário para prosseguir.
    """
    st.markdown("#### 📥 Lançar Nova Venda")
    data_venda_str = pd.to_datetime(data_lanc).strftime("%d/%m/%Y")
    st.caption(f"Data do lançamento: **{data_venda_str}**")

    # ------------------- Campos básicos -------------------
    valor = st.number_input(
        "Valor da Venda",
        min_value=0.0,
        step=0.01,
        format="%.2f",
        key="venda_valor",
        on_change=invalidate_confirm,
    )
    forma = st.selectbox(
        "Forma de Pagamento",
        ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"],
        key="venda_forma",
        on_change=invalidate_confirm,
    )

    parcelas, bandeira, maquineta = 1, "", ""
    banco_pix_direto: Optional[str] = None
    taxa_pix_direto: float = 0.0
    modo_pix: Optional[str] = None

    # ================= PIX =================
    if forma == "PIX":
        modo_pix = st.radio(
            "Como será o PIX?",
            ["Via maquineta", "Direto para banco"],
            horizontal=True,
            key="pix_modo",
            on_change=invalidate_confirm,
        )

        if modo_pix == "Via maquineta":
            try:
                with get_conn(caminho_banco) as conn:
                    maq_pix = pd.read_sql(
                        """
                        SELECT DISTINCT maquineta
                          FROM taxas_maquinas
                         WHERE UPPER(forma_pagamento)='PIX'
                         ORDER BY maquineta
                        """,
                        conn,
                    )["maquineta"].dropna().astype(str).tolist()
            except Exception:
                maq_pix = []

            if not maq_pix:
                st.warning("Nenhuma maquineta cadastrada para PIX. Cadastre em **Cadastro → Taxas por Maquineta**.")
                return None

            maquineta = st.selectbox(
                "PSP/Maquineta do PIX",
                maq_pix,
                key="pix_maquineta",
                on_change=invalidate_confirm,
            )

        else:  # Direto para banco
            try:
                with get_conn(caminho_banco) as conn:
                    bancos = pd.read_sql(
                        "SELECT nome FROM bancos_cadastrados ORDER BY nome",
                        conn,
                    )["nome"].dropna().astype(str).tolist()
            except Exception:
                bancos = []

            if not bancos:
                st.warning("Nenhum banco cadastrado. Cadastre em **Cadastro → Bancos**.")
                return None

            banco_pix_direto = st.selectbox(
                "Banco que receberá o PIX",
                bancos,
                key="pix_banco",
                on_change=invalidate_confirm,
            )

        taxa_pix_direto = 0.0

    # ============ Cartões e Link de Pagamento ============
    elif forma in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"]:
        formas = _formas_equivalentes(forma)
        placeholders = ",".join(["?"] * len(formas))
        try:
            with get_conn(caminho_banco) as conn:
                maq_por_forma = pd.read_sql(
                    f"""
                    SELECT DISTINCT maquineta
                      FROM taxas_maquinas
                     WHERE UPPER(forma_pagamento) IN ({placeholders})
                     ORDER BY maquineta
                    """,
                    conn,
                    params=[f.upper() for f in formas],
                )["maquineta"].dropna().astype(str).tolist()
        except Exception:
            maq_por_forma = []

        if not maq_por_forma:
            st.warning(f"Nenhuma maquineta cadastrada para **{forma}**. Cadastre em **Cadastro → Taxas por Maquineta**.")
            return None

        maquineta = st.selectbox(
            "Maquineta",
            maq_por_forma,
            key="cartao_maquineta",
            on_change=invalidate_confirm,
        )

        try:
            with get_conn(caminho_banco) as conn:
                bandeiras = pd.read_sql(
                    f"""
                    SELECT DISTINCT bandeira
                      FROM taxas_maquinas
                     WHERE UPPER(forma_pagamento) IN ({placeholders})
                       AND maquineta=?
                     ORDER BY bandeira
                    """,
                    conn,
                    params=[f.upper() for f in formas] + [maquineta],
                )["bandeira"].dropna().astype(str).tolist()
        except Exception:
            bandeiras = []

        if not bandeiras:
            st.warning(
                f"Nenhuma bandeira cadastrada para **{forma} / {maquineta}**. "
                "Cadastre em **Cadastro → Taxas por Maquineta**."
            )
            return None

        bandeira = st.selectbox(
            "Bandeira",
            bandeiras,
            key="cartao_bandeira",
            on_change=invalidate_confirm,
        )

        try:
            with get_conn(caminho_banco) as conn:
                pars = pd.read_sql(
                    f"""
                    SELECT DISTINCT parcelas
                      FROM taxas_maquinas
                     WHERE UPPER(forma_pagamento) IN ({placeholders})
                       AND maquineta=?
                       AND bandeira=?
                     ORDER BY parcelas
                    """,
                    conn,
                    params=[f.upper() for f in formas] + [maquineta, bandeira],
                )["parcelas"].dropna().astype(int).tolist()
        except Exception:
            pars = []

        if not pars:
            st.warning(
                f"Nenhuma parcela cadastrada para **{forma} / {maquineta} / {bandeira}**. "
                "Cadastre em **Cadastro → Taxas por Maquineta**."
            )
            return None

        parcelas = st.selectbox(
            "Parcelas",
            pars,
            key="cartao_parcelas",
            on_change=invalidate_confirm,
        )
        st.caption(f"Parcelas: **{int(parcelas or 1)}×**")

    else:
        st.caption("🧾 Venda em **dinheiro** será registrada no **Caixa**.")

    # ================= Resumo (card azul) =================
    linhas_md = [
        "**Confirme os dados da venda**",
        f"- **Data:** {data_venda_str}",
        f"- **Valor:** {formatar_moeda(valor)}",   # [usa helper central]
        f"- **Forma de pagamento:** {forma}",
    ]
    if forma in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"]:
        linhas_md += [
            f"- **Maquineta:** {maquineta or '—'}",
            f"- **Bandeira:** {bandeira or '—'}",
            f"- **Parcelas:** {int(parcelas or 1)}x",
        ]
    elif forma == "PIX":
        if modo_pix == "Via maquineta":
            linhas_md += [f"- **PIX via maquineta:** {maquineta or '—'}"]
        else:
            linhas_md += [
                f"- **PIX direto ao banco:** {banco_pix_direto or '—'}",
                f"- **Taxa informada (%):** {float(taxa_pix_direto or 0.0):.2f}",
            ]

    st.info("\n".join(linhas_md))  # card/resumo azul

    # ============ Confirmação (igual transferência) ============
    confirmado = st.checkbox("Está tudo certo com os dados acima?", key="venda_confirmar")

    # Frase informativa abaixo do checkbox / botão (padrão da transferência)
    st.info("Confirme os dados para habilitar o botão de salvar.")

    # Retorno para a página (que controla o botão 'Salvar Venda')
    return {
        "valor": float(valor or 0.0),
        "forma": (forma or "").strip(),
        "parcelas": int(parcelas or 1),
        "bandeira": (bandeira or "").strip(),
        "maquineta": (maquineta or "").strip(),
        "modo_pix": modo_pix if forma == "PIX" else None,
        "banco_pix_direto": (banco_pix_direto or "").strip() if banco_pix_direto else None,
        "taxa_pix_direto": float(taxa_pix_direto or 0.0),
        "confirmado": bool(confirmado),
    }
