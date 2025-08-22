# ===================== Page: Transferência =====================
"""
Página principal da Transferência – monta layout e chama forms/actions.
Preserva o comportamento do arquivo original: toggle, confirmação, validações,
mensagens e rerun após sucesso.
"""

import streamlit as st
from .state_transferencia import toggle_form, form_visivel
from .ui_forms_transferencia import render_form
from .actions_transferencia import registrar_transferencia, carregar_nomes_bancos

def render_page(caminho_banco: str, data_lanc):
    """
    Renderiza a página de Transferência Banco → Banco.

    Args:
        caminho_banco: Caminho do SQLite.
        data_lanc: Data do lançamento (date/str).
    """
    st.markdown("### 🔁 Transferência entre Bancos")

    # Toggle do formulário
    if st.button("🔁 Transferência entre Bancos", use_container_width=True, key="btn_transf_bancos_toggle"):
        toggle_form()

    if not form_visivel():
        return

    nomes_bancos = carregar_nomes_bancos(caminho_banco)
    form = render_form(data_lanc, nomes_bancos)
    if not form["submit"]:
        return

    # Trava extra de confirmação (servidor)
    if not st.session_state.get("transf_bancos_confirmar", False):
        st.warning("⚠️ Confirme os dados antes de salvar.")
        return

    # Validações equivalentes ao original
    if form["valor"] <= 0:
        st.warning("⚠️ Valor inválido.")
        return
    if not form["banco_origem"] or not form["banco_destino"]:
        st.warning("⚠️ Informe banco de origem e banco de destino.")
        return
    if form["banco_origem"].lower() == form["banco_destino"].lower():
        st.warning("⚠️ Origem e destino não podem ser o mesmo banco.")
        return

    try:
        res = registrar_transferencia(
            caminho_banco=caminho_banco,
            data_lanc=data_lanc,
            banco_origem_in=form["banco_origem"],
            banco_destino_in=form["banco_destino"],
            valor=form["valor"],
        )
        st.session_state["msg_ok"] = res["msg"]
        st.session_state.form_transf_bancos = False
        st.success(res["msg"])
        st.rerun()
    except RuntimeError as warn:
        st.warning(str(warn))
        st.session_state.form_transf_bancos = False
        st.rerun()
    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao registrar transferência: {e}")