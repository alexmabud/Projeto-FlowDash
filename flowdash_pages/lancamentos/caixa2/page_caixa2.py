# ===================== Page: Caixa 2 =====================
"""
Page: Caixa 2

Resumo:
    Página principal do Caixa 2: monta layout, exibe o formulário e dispara a ação
    de transferência (Caixa/Caixa Vendas → Caixa 2).

Fluxo:
    1) Resolve inputs (caminho do banco e data).
    2) Toggle do formulário via botão.
    3) Renderiza UI do form (data + valor) e aguarda confirmação.
    4) Valida o valor ( > 0 ).
    5) Chama a action `transferir_para_caixa2`, fecha o form e informa sucesso.

Entrada:
    - state (opcional): objeto com possíveis atributos (db_path/caminho_banco, data_lanc/...).
    - caminho_banco (opcional): caminho do SQLite (fallback).
    - data_lanc (opcional): date ou 'YYYY-MM-DD' (fallback).

Saída:
    - Nenhuma. Renderiza componentes Streamlit e mensagens de status.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Optional

import streamlit as st

from .state_caixa2 import toggle_form, form_visivel, close_form
from .ui_forms_caixa2 import render_form
from .actions_caixa2 import transferir_para_caixa2

__all__ = ["render_caixa2"]


def _coalesce_state(
    state: Any,
    caminho_banco: Optional[str],
    data_lanc: Optional[Any],
) -> tuple[str, str]:
    """Extrai (caminho_banco, data_lanc_str) com fallback e normaliza a data para 'YYYY-MM-DD'."""
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
    if dt is None:
        raise ValueError("Data do lançamento não informada (state.data_lanc / data_lanc).")

    if isinstance(dt, _dt.date):
        dt_str = dt.strftime("%Y-%m-%d")
    else:
        try:
            dt_str = _dt.datetime.strptime(str(dt), "%Y-%m-%d").strftime("%Y-%m-%d")
        except Exception:
            try:
                dt_iso = _dt.datetime.fromisoformat(str(dt)).date()
                dt_str = dt_iso.strftime("%Y-%m-%d")
            except Exception:
                raise ValueError(f"Data do lançamento inválida: {dt!r}")

    return str(db), dt_str


def render_caixa2(
    state: Any = None,
    caminho_banco: Optional[str] = None,
    data_lanc: Optional[Any] = None,
) -> None:
    """Renderiza a página de transferência para o Caixa 2 e executa a operação quando confirmada."""
    # 1) Inputs
    try:
        _db_path, _data_lanc = _coalesce_state(state, caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Configuração incompleta: {e}")
        return

    # 2) Toggle do formulário (padrão visual alinhado aos outros botões)
    if st.button("📦 Transferência para Caixa 2", use_container_width=True, key="btn_caixa2_toggle"):
        toggle_form()

    if not form_visivel():
        return

    # 3) Form UI (sem uso de session_state para data)
    form = render_form(_data_lanc)
    if not form.get("submit"):
        return

    # 4) Validação do valor
    try:
        v = float(form.get("valor", 0) or 0)
    except Exception:
        v = 0.0
    if v <= 0:
        st.warning("⚠️ Valor inválido.")
        return

    # 5) Resolve usuário logado (string mesmo se vier dict no session_state)
    usuario = (
        st.session_state.get("usuario_logado")
        or st.session_state.get("usuario")
        or st.session_state.get("username")
        or st.session_state.get("user")
        or "sistema"
    )
    if isinstance(usuario, dict):
        usuario = (
            usuario.get("nome")
            or usuario.get("name")
            or usuario.get("username")
            or usuario.get("email")
            or "sistema"
        )

    # 6) Executa a ação
    try:
        res = transferir_para_caixa2(
            caminho_banco=_db_path,
            data_lanc=_data_lanc,
            valor=v,
            usuario=usuario,
        )
        if isinstance(res, dict) and res.get("ok"):
            st.session_state["msg_ok"] = res.get("msg", "Transferência realizada.")
            close_form()
            st.success(res.get("msg", "Transferência realizada com sucesso."))

            # Limpa campos do form para evitar conflito no próximo render
            for k in ("caixa2_confirma_widget", "caixa2_valor"):
                if k in st.session_state:
                    del st.session_state[k]

            st.rerun()
        else:
            msg = (res or {}).get("msg") if isinstance(res, dict) else str(res)
            st.warning(f"⚠️ Não foi possível confirmar a operação. {msg or ''}".strip())
    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao transferir: {e}")