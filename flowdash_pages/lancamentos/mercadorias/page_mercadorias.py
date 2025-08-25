# ===================== Page: Mercadorias =====================
"""
Página principal de Mercadorias – monta layout e chama forms/actions.
- Dois fluxos: Compra e Recebimento
- Toggle, confirmação (apenas dentro do form), validações e rerun após sucesso
"""

from __future__ import annotations

import streamlit as st
from datetime import date
from utils.utils import coerce_data  # normaliza a data recebida

from .state_mercadorias import toggle_compra, toggle_receb, compra_visivel, receb_visivel
from .ui_forms_mercadorias import render_form_compra, render_form_recebimento
from .actions_mercadorias import salvar_compra, carregar_compras, salvar_recebimento

# Keys auxiliares (não há checkbox fora do form)
SHOW_TODAS_KEY     = "pg_merc_mostrar_todas"
SELECT_COMPRA_KEY  = "pg_merc_receb_sel"


def _is_confirmed(payload: dict, fallback_keys: list[str] = None) -> bool:
    """
    Lê a confirmação vinda de dentro do formulário.
    Prioriza payload['confirmado']; se não existir, tenta chaves no session_state.
    """
    if payload and isinstance(payload, dict):
        if "confirmado" in payload:
            return bool(payload.get("confirmado"))
    if fallback_keys:
        for k in fallback_keys:
            if bool(st.session_state.get(k, False)):
                return True
    return False


def render_mercadorias(caminho_banco: str, data_lanc=None):
    # --- Normaliza para datetime.date (aceita None/str/date) ---
    data_lanc: date = coerce_data(data_lanc)

    st.markdown("### 📦 Mercadorias")

    # ====== Compra ======
    if st.button("🧾 Compra de Mercadorias", use_container_width=True, key="btn_merc_compra_toggle"):
        toggle_compra()
        st.rerun()

    if compra_visivel():
        with st.form("form_merc_compra"):
            payload_compra = render_form_compra(data_lanc)

            confirmado_compra = _is_confirmed(
                payload_compra,
                # se seu form usar alguma key no session_state, inclua aqui:
                fallback_keys=["merc_compra_confirma_in", "merc_compra_confirma"]
            )

            submitted = st.form_submit_button(
                "💾 Salvar Compra",
                use_container_width=True,
                disabled=not confirmado_compra,
            )

        if submitted:
            if not confirmado_compra:
                st.warning("⚠️ Confirme os dados no formulário para salvar.")
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
        mostrar_todas = st.checkbox("Mostrar já recebidas", value=False, key=SHOW_TODAS_KEY)

        try:
            compras = carregar_compras(caminho_banco, incluir_recebidas=mostrar_todas)
        except Exception as e:
            st.error(f"Erro ao carregar compras: {e}")
            return

        if not compras:
            st.info("Nenhuma compra pendente de recebimento.")
            return

        label_map = {
            c["id"]: f"#{c['id']} • {c.get('Data','—')} • {c.get('Fornecedor','—')} • {c.get('Colecao','—')} • Pedido:{c.get('Pedido','—')}"
            for c in compras if "id" in c
        }
        if not label_map:
            st.info("Não há itens válidos para receber.")
            return

        selected_id = st.selectbox(
            "Selecione a compra",
            options=list(label_map.keys()),
            format_func=lambda k: label_map[k],
            key=SELECT_COMPRA_KEY,
        )

        with st.form("form_merc_receb"):
            payload_receb = render_form_recebimento(data_lanc, compras, selected_id)

            confirmado_receb = _is_confirmed(
                payload_receb,
                fallback_keys=["merc_receb_confirma_in", "merc_receb_confirma"]
            )

            submitted_r = st.form_submit_button(
                "💾 Salvar Recebimento",
                use_container_width=True,
                disabled=not confirmado_receb,
            )

        if submitted_r:
            if not confirmado_receb:
                st.warning("⚠️ Confirme os dados no formulário para salvar.")
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
