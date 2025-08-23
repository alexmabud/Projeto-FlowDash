# ===================== Page: Depósito =====================
"""
Página principal do Depósito – monta layout e chama forms/actions.
Preserva o comportamento do arquivo original: toggle, confirmação, validações,
mensagens e rerun após sucesso.
"""

import streamlit as st
from .state_deposito import toggle_form, form_visivel
from .ui_forms_deposito import render_form
from .actions_deposito import registrar_deposito, carregar_nomes_bancos

def render_deposito(caminho_banco: str, data_lanc):
    """
    Renderiza a página de Depósito.

    Args:
        caminho_banco: Caminho do SQLite.
        data_lanc: Data do lançamento (date/str).
    """
    st.markdown("### 🏦 Depósito Bancário")

    # Toggle do formulário (mesmo comportamento do original)
    if st.button("🏦 Depósito Bancário", use_container_width=True, key="btn_deposito_toggle"):
        toggle_form()

    if not form_visivel():
        return

    nomes_bancos = carregar_nomes_bancos(caminho_banco)
    form = render_form(data_lanc, nomes_bancos)
    if not form["submit"]:
        return

    # Trava extra de confirmação (servidor)
    if not st.session_state.get("deposito_confirmar", False):
        st.warning("⚠️ Confirme os dados antes de salvar.")
        return

    # Validações equivalentes ao original
    if (form["valor"] or 0.0) <= 0:
        st.warning("⚠️ Valor inválido.")
        return
    if not form["banco_escolhido"]:
        st.warning("⚠️ Selecione ou digite o banco de destino.")
        return

    try:
        res = registrar_deposito(
            caminho_banco=caminho_banco,
            data_lanc=data_lanc,
            valor=form["valor"],
            banco_in=form["banco_escolhido"],
        )
        # Aviso de possível falha no upsert de saldos_bancos é propagado como RuntimeError
        st.session_state["msg_ok"] = res["msg"]
        st.session_state.form_deposito = False
        st.success(res["msg"])
        st.rerun()
    except RuntimeError as warn:  # falha em saldos_bancos
        st.warning(str(warn))
        st.session_state.form_deposito = False
        st.rerun()
    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao registrar depósito: {e}")
