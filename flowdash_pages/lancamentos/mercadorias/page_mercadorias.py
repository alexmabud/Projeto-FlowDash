# ===================== Page: Mercadorias =====================
"""
Página principal de Mercadorias – monta layout e chama forms/actions.
- Dois fluxos: Compra e Recebimento
- Toggle, confirmação, validações e rerun após sucesso
"""

from __future__ import annotations

import streamlit as st
from datetime import date
from .state_mercadorias import toggle_compra, toggle_receb, compra_visivel, receb_visivel
from .ui_forms_mercadorias import render_form_compra, render_form_recebimento
from .actions_mercadorias import salvar_compra, carregar_compras, salvar_recebimento

def render_mercadorias(caminho_banco: str, data_lanc: date):
    st.markdown("### 📦 Mercadorias")

    # ====== Compra ======
    if st.button("🧾 Compra de Mercadorias", use_container_width=True, key="btn_merc_compra_toggle"):
        toggle_compra()
        st.rerun()

    if compra_visivel():
        # form
        with st.form("form_merc_compra"):
            payload_compra = render_form_compra(data_lanc)
            submitted = st.form_submit_button(
                "💾 Salvar Compra",
                use_container_width=True,
                disabled=not bool(st.session_state.get("merc_compra_confirma_out", False))
            )
        # checkbox também fica visível fora do form (mesmo UX do original)
        st.checkbox("Confirmo os dados", key="merc_compra_confirma_out")

        if submitted:
            if not st.session_state.get("merc_compra_confirma_out", False):
                st.warning("⚠️ Marque 'Confirmo os dados' para salvar.")
            else:
                try:
                    msg = salvar_compra(caminho_banco, payload_compra)
                    st.session_state["msg_ok"] = msg
                    st.session_state["show_merc_compra"] = False
                    st.rerun()
                except ValueError as ve:
                    st.warning(f"⚠️ {ve}")
                except Exception as e:
                    st.error(f"❌ Erro ao salvar compra: {e}")

    st.divider()

    # ====== Recebimento ======
    if st.button("📥 Recebimento de Mercadorias", use_container_width=True, key="btn_merc_receb_toggle"):
        toggle_receb()
        st.rerun()

    if receb_visivel():
        mostrar_todas = st.checkbox("Mostrar já recebidas", value=False, key="chk_merc_mostrar_todas")

        try:
            compras = carregar_compras(caminho_banco, incluir_recebidas=mostrar_todas)
        except Exception as e:
            st.error(f"Erro ao carregar compras: {e}")
            return

        if not compras:
            st.info("Nenhuma compra pendente de recebimento.")
            return

        label_map = {c["id"]: f"#{c['id']} • {c['Data']} • {c['Fornecedor']} • {c['Colecao']} • Pedido:{c['Pedido']}" for c in compras}
        selected_id = st.selectbox(
            "Selecione a compra",
            options=list(label_map.keys()),
            format_func=lambda k: label_map[k],
            key="merc_receb_sel"
        )

        with st.form("form_merc_receb"):
            payload_receb = render_form_recebimento(data_lanc, compras, selected_id)
            submitted_r = st.form_submit_button(
                "💾 Salvar Recebimento",
                use_container_width=True,
                disabled=not bool(st.session_state.get("merc_receb_confirma_out", False))
            )
        st.checkbox("Confirmo os dados", key="merc_receb_confirma_out")

        if submitted_r:
            if not st.session_state.get("merc_receb_confirma_out", False):
                st.warning("⚠️ Marque 'Confirmo os dados' para salvar.")
            else:
                try:
                    msg = salvar_recebimento(caminho_banco, payload_receb)
                    st.session_state["msg_ok"] = msg
                    st.session_state["show_merc_receb"] = False
                    st.rerun()
                except ValueError as ve:
                    st.warning(f"⚠️ {ve}")
                except Exception as e:
                    st.error(f"❌ Erro ao salvar recebimento: {e}")
