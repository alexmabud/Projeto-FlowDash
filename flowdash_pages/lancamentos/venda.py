import streamlit as st
import pandas as pd
from .shared import (
    get_conn, DIAS_COMPENSACAO, proximo_dia_util_br,
    obter_banco_destino
)
from utils.utils import formatar_valor
from services.vendas import VendasService

def render_venda(caminho_banco: str, data_lanc):
    if st.button("🟢 Nova Venda", use_container_width=True, key="btn_venda_toggle"):
        st.session_state.form_venda = not st.session_state.get("form_venda", False)

    if not st.session_state.get("form_venda", False):
        return

    st.markdown("#### 📋 Nova Venda")

    valor = st.number_input("Valor da Venda", min_value=0.0, step=0.01, key="venda_valor", format="%.2f")
    forma = st.selectbox("Forma de Pagamento", ["DINHEIRO","PIX","DÉBITO","CRÉDITO","LINK_PAGAMENTO"], key="venda_forma")

    parcelas, bandeira, maquineta = 1, "", ""
    banco_pix_direto, taxa_pix_direto = None, 0.0

    # maquinetas cadastradas (só leitura)
    try:
        with get_conn(caminho_banco) as conn:
            maq = pd.read_sql("SELECT DISTINCT maquineta FROM taxas_maquinas ORDER BY maquineta", conn)["maquineta"].tolist()
    except Exception:
        maq = []

    if forma == "PIX":
        modo_pix = st.radio("Como será o PIX?", ["Via maquineta","Direto para banco"], horizontal=True, key="pix_modo")
        if modo_pix == "Via maquineta":
            with get_conn(caminho_banco) as conn:
                maq_pix = pd.read_sql(
                    "SELECT DISTINCT maquineta FROM taxas_maquinas WHERE forma_pagamento='PIX' ORDER BY maquineta",
                    conn
                )["maquineta"].tolist()
            maquineta = st.selectbox("PSP/Maquineta do PIX", maq_pix, key="pix_maquineta")
        else:
            with get_conn(caminho_banco) as conn:
                try:
                    bancos = pd.read_sql("SELECT nome FROM bancos_cadastrados ORDER BY nome", conn)["nome"].tolist()
                except Exception:
                    bancos = []
            banco_pix_direto = st.selectbox("Banco que receberá o PIX", bancos, key="pix_banco")
            taxa_pix_direto  = st.number_input("Taxa do PIX direto (%)", min_value=0.0, step=0.01, value=0.0, format="%.2f", key="pix_taxa")

    elif forma in ["DÉBITO","CRÉDITO","LINK_PAGAMENTO"]:
        maquineta = st.selectbox("Maquineta", maq, key="cartao_maquineta")
        with get_conn(caminho_banco) as conn:
            bandeiras = pd.read_sql(
                """
                SELECT DISTINCT bandeira FROM taxas_maquinas
                WHERE forma_pagamento=? AND maquineta=?
                ORDER BY bandeira
                """,
                conn,
                params=(forma if forma!="LINK_PAGAMENTO" else "CRÉDITO", maquineta)
            )["bandeira"].tolist()
        bandeira = st.selectbox("Bandeira", bandeiras, key="cartao_bandeira") if bandeiras else ""
        if forma in ["CRÉDITO","LINK_PAGAMENTO"]:
            with get_conn(caminho_banco) as conn:
                pars = pd.read_sql(
                    """
                    SELECT DISTINCT parcelas FROM taxas_maquinas
                    WHERE forma_pagamento=? AND maquineta=? AND bandeira=?
                    ORDER BY parcelas
                    """,
                    conn,
                    params=(forma if forma!="LINK_PAGAMENTO" else "CRÉDITO", maquineta, bandeira)
                )["parcelas"].tolist()
            parcelas = st.selectbox("Parcelas", pars if pars else [1], key="cartao_parcelas")
    else:
        st.caption("🧾 Venda em dinheiro será registrada no **Caixa**.")

    # ================== RESUMO (apenas o que o usuário preencheu) ==================
    data_venda_str = pd.to_datetime(data_lanc).strftime("%d/%m/%Y")
    linhas_md = [
        "**Confirme os dados da venda**",
        f"- **Data:** {data_venda_str}",
        f"- **Valor:** {formatar_valor(valor or 0.0)}",
        f"- **Forma de pagamento:** {forma}",
    ]

    if forma in ["DÉBITO","CRÉDITO","LINK_PAGAMENTO"]:
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
    # Para DINHEIRO não há campos adicionais

    st.info("\n".join(linhas_md))
    # =======================================================================

    # Confirmação única do formulário
    confirmar = st.checkbox("Confirmo os dados acima", key="venda_confirmar")

    if st.button("💾 Salvar Venda", use_container_width=True, key="venda_salvar"):
        # validações básicas
        if valor <= 0:
            st.warning("⚠️ Valor inválido.")
            return
        if not confirmar:
            st.warning("⚠️ Confirme os dados antes de salvar.")
            return
        if forma in ["DÉBITO","CRÉDITO","LINK_PAGAMENTO"] and (not maquineta or not bandeira):
            st.warning("⚠️ Selecione maquineta e bandeira.")
            return
        if forma == "PIX" and st.session_state.get("pix_modo") == "Via maquineta" and not maquineta:
            st.warning("⚠️ Selecione a maquineta do PIX.")
            return
        if forma == "PIX" and st.session_state.get("pix_modo") == "Direto para banco" and not banco_pix_direto:
            st.warning("⚠️ Selecione o banco que receberá o PIX direto.")
            return

        # ===== taxa + banco_destino (da base, sem mostrar no resumo)
        taxa, banco_destino = 0.0, None
        if forma in ["DÉBITO","CRÉDITO","LINK_PAGAMENTO"]:
            with get_conn(caminho_banco) as conn:
                row = conn.execute(
                    """
                    SELECT taxa_percentual, banco_destino FROM taxas_maquinas
                    WHERE forma_pagamento=? AND maquineta=? AND bandeira=? AND parcelas=?
                    LIMIT 1
                    """,
                    (forma if forma!="LINK_PAGAMENTO" else "CRÉDITO", maquineta, bandeira, int(parcelas or 1))
                ).fetchone()
            if row:
                taxa = float(row[0] or 0.0)
                banco_destino = row[1] or None
            if not banco_destino:
                banco_destino = obter_banco_destino(caminho_banco, forma, maquineta, bandeira, parcelas)

        elif forma == "PIX":
            if st.session_state.get("pix_modo") == "Via maquineta":
                with get_conn(caminho_banco) as conn:
                    row = conn.execute(
                        """
                        SELECT taxa_percentual, banco_destino FROM taxas_maquinas
                        WHERE forma_pagamento='PIX' AND maquineta=? AND bandeira='' AND parcelas=1
                        LIMIT 1
                        """,
                        (maquineta,)
                    ).fetchone()
                taxa = float(row[0] or 0.0) if row else 0.0
                banco_destino = (row[1] if row and row[1] else None) or obter_banco_destino(caminho_banco, "PIX", maquineta, "", 1)
            else:
                banco_destino = banco_pix_direto
                taxa = float(taxa_pix_direto or 0.0)

        else:  # DINHEIRO
            banco_destino, taxa, parcelas, bandeira, maquineta = None, 0.0, 1, "", ""

        # ===== calcula data de liquidação (UI decide; não exibimos no resumo)
        base = pd.to_datetime(data_lanc).date()
        dias = DIAS_COMPENSACAO.get(forma, 0)
        data_liq = proximo_dia_util_br(base, dias) if dias > 0 else base

        # ===== chama o service
        usuario = st.session_state.usuario_logado["nome"] if "usuario_logado" in st.session_state and st.session_state.usuario_logado else "Sistema"
        service = VendasService(caminho_banco)

        try:
            venda_id, mov_id = service.registrar_venda(
                data_venda=str(data_lanc),
                data_liq=str(data_liq),
                valor_bruto=float(valor),
                forma=forma,
                parcelas=int(parcelas or 1),
                bandeira=bandeira or "",
                maquineta=maquineta or "",
                banco_destino=banco_destino,     # None p/ dinheiro; nome p/ demais
                taxa_percentual=float(taxa or 0.0),
                usuario=usuario
            )

            if venda_id == -1:
                st.session_state["msg_ok"] = "⚠️ Venda já registrada (idempotência)."
            else:
                valor_liq = float(valor) * (1 - float(taxa or 0.0)/100.0)
                msg_liq = (
                    f"Liquidação de {formatar_valor(valor_liq)} em {(banco_destino or 'Caixa')} "
                    f"em {pd.to_datetime(data_liq).strftime('%d/%m/%Y')}"
                )
                st.session_state["msg_ok"] = f"✅ Venda registrada! {msg_liq}"

            st.session_state.form_venda = False
            st.rerun()

        except Exception as e:
            st.error(f"Erro ao salvar venda: {e}")