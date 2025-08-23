# ===================== Page: Saída =====================
"""
Página principal da Saída – monta layout e chama forms/actions.

Mantém o comportamento do arquivo original:
- Toggle do formulário
- Campos e fluxos idênticos (incluindo Pagamentos: Fatura/Boletos/Empréstimos)
- Validações e mensagens
- st.rerun() após sucesso
"""

from __future__ import annotations

import streamlit as st
from datetime import date

from .state_saida import toggle_form, form_visivel, invalidate_confirm
from .ui_forms_saida import render_form_saida
from .actions_saida import (
    carregar_listas_para_form,
    registrar_saida,
)

def render_saida(caminho_banco: str, data_lanc: date):
    """
    Renderiza a página de Saída.

    Args:
        caminho_banco: Caminho do SQLite.
        data_lanc: Data do lançamento (date).
    """
    st.markdown("### 🔴 Saída")

    # Toggle do formulário (mesmo comportamento do original)
    if st.button("🔴 Saída", use_container_width=True, key="btn_saida_toggle"):
        toggle_form()

    if not form_visivel():
        return

    # Contexto do usuário
    usuario = st.session_state.get("usuario_logado", {"nome": "Sistema"})
    usuario_nome = usuario.get("nome", "Sistema")

    # Carrega listas/repos necessárias para o formulário
    (
        nomes_bancos,
        nomes_cartoes,
        df_categorias,
        listar_subcategorias_fn,
        listar_destinos_fatura_em_aberto_fn,
        carregar_opcoes_pagamentos_fn,
    ) = carregar_listas_para_form(caminho_banco)

    # Render UI (retorna payload com todos os campos)
    payload = render_form_saida(
        data_lanc=data_lanc,
        invalidate_cb=invalidate_confirm,
        nomes_bancos=nomes_bancos,
        nomes_cartoes=nomes_cartoes,
        categorias_df=df_categorias,
        listar_subcategorias_fn=listar_subcategorias_fn,
        listar_destinos_fatura_em_aberto_fn=listar_destinos_fatura_em_aberto_fn,
        carregar_opcoes_pagamentos_fn=lambda tipo: carregar_opcoes_pagamentos_fn(tipo),
    )

    # Botão salvar: mesma trava do original
    save_disabled = not st.session_state.get("confirmar_saida", False)
    if not st.button("💾 Salvar Saída", use_container_width=True, key="btn_salvar_saida", disabled=save_disabled):
        return

    # Segurança no servidor
    if not st.session_state.get("confirmar_saida", False):
        st.warning("⚠️ Confirme os dados antes de salvar.")
        return

    try:
        res = registrar_saida(
            caminho_banco=caminho_banco,
            data_lanc=data_lanc,
            usuario_nome=usuario_nome,
            payload=payload,
        )

        # Feedbacks idênticos aos do original
        st.session_state["msg_ok"] = res["msg"]

        # Info de classificação (somente para Pagamentos fora de Boletos)
        if payload.get("is_pagamentos") and payload.get("tipo_pagamento_sel") != "Boletos":
            st.info(f"Destino classificado: {payload.get('tipo_pagamento_sel')} → {payload.get('destino_pagamento_sel') or '—'}")

        st.session_state.form_saida = False
        st.success(res["msg"])
        st.rerun()

    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"Erro ao salvar saída: {e}")
