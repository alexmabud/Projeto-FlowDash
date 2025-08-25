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

from typing import Any, Optional, Tuple
import datetime as _dt
import streamlit as st

from utils.utils import coerce_data  # <<< normaliza/garante datetime.date

from .state_saida import toggle_form, form_visivel, invalidate_confirm
from .ui_forms_saida import render_form_saida
from .actions_saida import (
    carregar_listas_para_form,
    registrar_saida,
)

__all__ = ["render_saida"]


# ----------------- helpers -----------------
def _norm_date(d: Any) -> _dt.date:
    """
    Normaliza data para datetime.date.
    Aceita: date/datetime/string ('YYYY-MM-DD', ISO, 'DD/MM/YYYY', 'DD-MM-YYYY') ou None (usa hoje).
    """
    # usa utilitário central do projeto (já cobre os formatos comuns)
    return coerce_data(d)


def _coalesce_state(
    state: Any,
    caminho_banco: Optional[str],
    data_lanc: Optional[Any],
) -> Tuple[str, _dt.date]:
    """Extrai (db_path, data_lanc: date) de state com fallback para args diretos."""
    db = None
    dt = None
    if state is not None:
        db = getattr(state, "db_path", None) or getattr(state, "caminho_banco", None)
        dt = (
            getattr(state, "data_lanc", None)
            or getattr(state, "data_lancamento", None)
            or getattr(state, "data", None)
        )
    db = db or caminho_banco
    dt = dt or data_lanc
    if not db:
        raise ValueError("Caminho do banco não informado (state.db_path / caminho_banco).")
    # Se dt vier None, coerce_data usa hoje por padrão
    return str(db), _norm_date(dt)


# ----------------- página -----------------
def render_saida(
    state: Any = None,
    caminho_banco: Optional[str] = None,
    data_lanc: Optional[Any] = None,
) -> None:
    """
    Preferencial:
        render_saida(state)

    Compatível:
        render_saida(None, caminho_banco='...', data_lanc=date|'YYYY-MM-DD')
    """
    # Resolver entradas
    try:
        _db_path, _data_lanc = _coalesce_state(state, caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Configuração incompleta: {e}")
        return

    # Toggle do formulário
    if st.button("🔴 Saída", use_container_width=True, key="btn_saida_toggle"):
        toggle_form()

    if not form_visivel():
        return

    # Contexto do usuário
    usuario = st.session_state.get("usuario_logado", {"nome": "Sistema"})
    usuario_nome = usuario.get("nome", "Sistema")

    # Carrega listas/repos necessárias para o formulário
    try:
        (
            nomes_bancos,
            nomes_cartoes,
            df_categorias,
            listar_subcategorias_fn,
            listar_destinos_fatura_em_aberto_fn,
            carregar_opcoes_pagamentos_fn,
        ) = carregar_listas_para_form(_db_path)
    except Exception as e:
        st.error(f"❌ Falha ao preparar formulário: {e}")
        return

    # Render UI (retorna payload com todos os campos)
    payload = render_form_saida(
        data_lanc=_data_lanc,  # <<< agora é datetime.date, não string
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

    # Execução
    try:
        res = registrar_saida(
            caminho_banco=_db_path,
            data_lanc=_data_lanc,  # <<< passa date para quem for usar .strftime(...)
            usuario_nome=usuario_nome,
            payload=payload,
        )

        # Feedbacks idênticos aos do original
        st.session_state["msg_ok"] = res["msg"]

        # Info de classificação (somente para Pagamentos fora de Boletos)
        if payload.get("is_pagamentos") and payload.get("tipo_pagamento_sel") != "Boletos":
            st.info(
                f"Destino classificado: {payload.get('tipo_pagamento_sel')} → "
                f"{payload.get('destino_pagamento_sel') or '—'}"
            )

        st.session_state.form_saida = False
        st.success(res["msg"])
        st.rerun()

    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao salvar saída: {e}")
