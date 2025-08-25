# ===================== Page: Venda =====================
"""
Página principal de Venda – monta layout e chama forms/actions.
Preserva o comportamento do arquivo original: toggle, confirmação, validações,
mensagens e rerun após sucesso.
"""

from __future__ import annotations

from datetime import date
import time
import streamlit as st

from utils.utils import coerce_data  # <<< normaliza a data recebida

from .state_venda import toggle_form, form_visivel
from .ui_forms_venda import render_form_venda
from .actions_venda import registrar_venda

__all__ = ["render_venda"]


def render_venda(caminho_banco: str, data_lanc=None) -> None:
    """
    Renderiza a página de Venda.

    Args:
        caminho_banco: Caminho do SQLite.
        data_lanc: Data do lançamento (opcional). Aceita None, datetime.date,
                   'YYYY-MM-DD', 'DD/MM/YYYY' ou 'DD-MM-YYYY'.
                   Se None/vazio, usa a data de hoje.
    """
    # --- Normaliza para datetime.date (evita erro de .strftime em string) ---
    data_lanc: date = coerce_data(data_lanc)

    # Toggle
    if st.button("🟢 Nova Venda", use_container_width=True, key="btn_venda_toggle"):
        toggle_form()

    if not form_visivel():
        return

    # UI retorna todos os campos + 'confirmado'
    try:
        form = render_form_venda(caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Falha ao montar formulário: {e}")
        return

    if not form:
        return  # UI já deu aviso correspondente

    # Botão salvar (mesma trava do original: exige confirmação)
    if not form.get("confirmado"):
        st.button("💾 Salvar Venda", use_container_width=True, key="venda_salvar", disabled=True)
        return

    if not st.button("💾 Salvar Venda", use_container_width=True, key="venda_salvar_ok"):
        return

    # Execução
    try:
        res = registrar_venda(
            db_like=caminho_banco,   # ✅ alinha com a nova API
            data_lanc=data_lanc,
            payload=form,
        )

        if res.get("ok"):
            st.session_state["msg_ok"] = res.get("msg", "Venda registrada.")
            st.session_state.form_venda = False
            st.success(res.get("msg", "Venda registrada com sucesso."))

            # 🔄 força o Recarregamento do Resumo do Dia / cards
            st.session_state["_resumo_dirty"] = time.time()

            # ✅ limpa caches de @st.cache_data para garantir recomputo do resumo
            st.cache_data.clear()

            st.rerun()
        else:
            st.error(res.get("msg") or "Erro ao salvar a venda.")

    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao salvar venda: {e}")
