# ===================== Page: Mercadorias =====================
"""Página principal de Mercadorias.

Responsável por montar o layout e orquestrar os formulários de:
- Compra de Mercadorias
- Recebimento de Mercadorias

Regras:
    - O botão "Salvar" só habilita após o usuário marcar "Confirmo os dados"
      dentro de cada formulário.
    - A mensagem de sucesso é exibida no banner global do app (hub), usando
      `st.session_state["msg_ok"]` e `st.session_state["msg_ok_type"]`.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

import streamlit as st
from utils.utils import coerce_data

from .actions_mercadorias import carregar_compras, salvar_compra, salvar_recebimento
from .state_mercadorias import compra_visivel, receb_visivel, toggle_compra, toggle_receb
from .ui_forms_mercadorias import render_form_compra, render_form_recebimento

# --- Keys auxiliares (estado da página) ---
SHOW_TODAS_KEY = "pg_merc_mostrar_todas"
SELECT_COMPRA_KEY = "pg_merc_receb_sel"


def _inject_borderless_css() -> None:
    """Remove bordas visuais de expanders para manter estilo “solto”."""
    st.markdown(
        """
        <style>
          div[data-testid="stExpander"] {
            border: 0 !important;
            background: transparent !important;
            box-shadow: none !important;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _limpar_campos_recebimento_do_payload(d: Dict[str, Any]) -> Dict[str, Any]:
    """Remove do payload de COMPRA os campos exclusivos de RECEBIMENTO.

    Args:
        d: Dicionário retornado pelo formulário de compra.

    Returns:
        Cópia do dicionário sem campos de recebimento.
    """
    if not isinstance(d, dict):
        return d
    campos_receb = [
        "Faturamento",
        "Recebimento",
        "Valor_Recebido",
        "Frete_Cobrado",
        "Recebimento_Obs",
        # aliases do form de recebimento
        "fat_dt",
        "rec_dt",
        "valor_recebido",
        "frete_cobrado",
        "obs",
    ]
    out = dict(d)  # cópia rasa para não mutar o original
    for k in campos_receb:
        out.pop(k, None)
    return out


def render_mercadorias(caminho_banco: str, data_lanc: Optional[date] = None) -> None:
    """Renderiza a página de Mercadorias (Compra e Recebimento).

    Args:
        caminho_banco: Caminho para o arquivo do banco sqlite.
        data_lanc: Data padrão a ser usada nos formulários (opcional).
    """
    data_lanc = coerce_data(data_lanc)
    _inject_borderless_css()

    # ====== Toggle: Compra ======
    if st.button("🧾 Compra de Mercadorias", use_container_width=True, key="btn_merc_compra_toggle"):
        toggle_compra()
        st.rerun()

    if compra_visivel():
        payload_compra = render_form_compra(data_lanc)
        confirmado_compra = bool(payload_compra.get("confirmado", False))

        salvar_c = st.button(
            "💾 Salvar Compra",
            use_container_width=True,
            disabled=not confirmado_compra,
            key="btn_salvar_compra",
        )

        if salvar_c:
            try:
                # Limpa campos indevidos antes de persistir COMPRA
                payload_compra_limpo = _limpar_campos_recebimento_do_payload(payload_compra)
                msg = salvar_compra(caminho_banco, payload_compra_limpo)
                # Banner global no hub:
                st.session_state["msg_ok"] = msg
                st.session_state["msg_ok_type"] = "success"
                st.session_state["show_merc_compra"] = False
                st.rerun()
            except ValueError as ve:
                st.warning(f"⚠️ {ve}")
            except Exception as e:
                st.error(f"❌ Erro ao salvar compra: {e}")

    # ====== Toggle: Recebimento ======
    if st.button("📥 Recebimento de Mercadorias", use_container_width=True, key="btn_merc_receb_toggle"):
        toggle_receb()
        st.rerun()

    if receb_visivel():
        mostrar_todas = st.checkbox("Mostrar já recebidas", value=False, key=SHOW_TODAS_KEY)

        # Carrega lista de compras (pendentes ou todas)
        try:
            compras: List[Dict[str, Any]] = carregar_compras(caminho_banco, incluir_recebidas=mostrar_todas)
        except Exception as e:
            st.error(f"Erro ao carregar compras: {e}")
            return

        if not compras:
            st.info("Nenhuma compra pendente de recebimento.")
            return

        # Monta rótulos do seletor
        label_map = {
            c["id"]: f"#{c['id']} • {c.get('Data','—')} • {c.get('Fornecedor','—')} • "
                     f"{c.get('Colecao','—')} • Pedido:{c.get('Pedido','—')}"
            for c in compras
            if "id" in c
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

        payload_receb = render_form_recebimento(data_lanc, compras, selected_id)
        confirmado_receb = bool(payload_receb and payload_receb.get("confirmado", False))

        salvar_r = st.button(
            "💾 Salvar Recebimento",
            use_container_width=True,
            disabled=not confirmado_receb,
            key="btn_salvar_receb",
        )

        if salvar_r:
            try:
                msg = salvar_recebimento(caminho_banco, payload_receb)
                # Banner global no hub:
                st.session_state["msg_ok"] = msg
                st.session_state["msg_ok_type"] = "success"
                st.session_state["show_merc_receb"] = False
                st.rerun()
            except ValueError as ve:
                st.warning(f"⚠️ {ve}")
            except Exception as e:
                st.error(f"❌ Erro ao salvar recebimento: {e}")
