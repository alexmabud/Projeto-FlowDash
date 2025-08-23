# ===================== Page: Caixa 2 =====================
"""
Página principal do Caixa 2 – monta layout e chama forms/actions.

Mantém o comportamento do arquivo original:
- Toggle do formulário
- Validação de valor > 0
- Mensagens de aviso/erro/sucesso
- st.rerun() após sucesso
"""

import streamlit as st
from .state_caixa2 import toggle_form, form_visivel
from .ui_forms_caixa2 import render_form
from .actions_caixa2 import transferir_para_caixa2

def render_caixa2(caminho_banco: str, data_lanc):
    """
    Renderiza a página do Caixa 2 (transferência para Caixa 2).

    Args:
        caminho_banco: Caminho do arquivo SQLite.
        data_lanc: Data do lançamento (date/str).
    """
    st.markdown("### 💼 Caixa 2")

    # Toggle do formulário (mesmo comportamento do original)
    if st.button("🔄 Caixa 2", use_container_width=True, key="btn_caixa2_toggle"):
        toggle_form()

    if not form_visivel():
        return

    form = render_form()
    if not form["submit"]:
        return

    # Validação equivalente ao original
    if form["valor"] <= 0:
        st.warning("⚠️ Valor inválido.")
        return

    try:
        res = transferir_para_caixa2(
            caminho_banco=caminho_banco,
            data_lanc=data_lanc,
            valor=form["valor"],
        )
        if res["ok"]:
            st.session_state["msg_ok"] = res["msg"]
            st.session_state.form_caixa2 = False
            st.success(res["msg"])
            st.rerun()
    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao transferir: {e}")
