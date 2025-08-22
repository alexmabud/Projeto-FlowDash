# ===================== Page: Venda =====================
"""
Página principal de Venda – monta layout e chama forms/actions.
Preserva o comportamento do arquivo original: toggle, confirmação, validações,
mensagens e rerun após sucesso.
"""

from __future__ import annotations

import streamlit as st
from .state_venda import toggle_form, form_visivel
from .ui_forms_venda import render_form_venda
from .actions_venda import registrar_venda

def render_page(caminho_banco: str, data_lanc):
    """
    Renderiza a página de Venda.

    Args:
        caminho_banco: Caminho do SQLite.
        data_lanc: Data do lançamento (date/str).
    """
    st.markdown("### 🟢 Nova Venda")

    # Toggle
    if st.button("🟢 Nova Venda", use_container_width=True, key="btn_venda_toggle"):
        toggle_form()

    if not form_visivel():
        return

    form = render_form_venda(caminho_banco, data_lanc)
    if form is None:
        return  # UI já deu o aviso correspondente

    # Botão salvar (mesma trava do original)
    if not form.get("confirmado"):
        st.button("💾 Salvar Venda", use_container_width=True, key="venda_salvar", disabled=True)
        return

    salvar_btn = st.button("💾 Salvar Venda", use_container_width=True, key="venda_salvar_ok", disabled=False)
    if not salvar_btn:
        return

    try:
        # injeta usuário (como no original)
        if "usuario_logado" in st.session_state and st.session_state.usuario_logado:
            usuario = st.session_state.usuario_logado.get("nome", "Sistema")
        else:
            usuario = "Sistema"

        # A action mantém a mesma lógica; só usamos usuário no service no original.
        res = registrar_venda(caminho_banco=caminho_banco, data_lanc=data_lanc, payload=form)

        st.session_state["msg_ok"] = res["msg"]
        st.session_state.form_venda = False
        st.success(res["msg"])
        st.rerun()

    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"Erro ao salvar venda: {e}")
