# ===================== Page: Caixa 2 =====================
"""
Página principal do Caixa 2 – monta layout e chama forms/actions.

Mantém o comportamento do arquivo original:
- Toggle do formulário
- Validação de valor > 0
- Mensagens de aviso/erro/sucesso
- st.rerun() após sucesso
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Optional

import streamlit as st

from .state_caixa2 import toggle_form, form_visivel
from .ui_forms_caixa2 import render_form
from .actions_caixa2 import transferir_para_caixa2

__all__ = ["render_caixa2"]


def _coalesce_state(
    state: Any,
    caminho_banco: Optional[str],
    data_lanc: Optional[Any],
) -> tuple[str, str]:
    """
    Extrai (caminho_banco, data_lanc_str) de `state` com fallback para parâmetros explícitos.
    data_lanc é normalizado para 'YYYY-MM-DD'.
    """
    # 1) tentar via state.*
    db = None
    dt = None
    if state is not None:
        # nomes comuns usados no projeto
        db = getattr(state, "db_path", None) or getattr(state, "caminho_banco", None)
        dt = (
            getattr(state, "data_lanc", None)
            or getattr(state, "data_lancamento", None)
            or getattr(state, "data", None)
        )

    # 2) fallback para argumentos diretos
    db = db or caminho_banco
    dt = dt or data_lanc

    if not db:
        raise ValueError("Caminho do banco não informado (state.db_path / caminho_banco).")
    if dt is None:
        raise ValueError("Data do lançamento não informada (state.data_lanc / data_lanc).")

    # normalizar data para string YYYY-MM-DD
    if isinstance(dt, _dt.date):
        dt_str = dt.strftime("%Y-%m-%d")
    else:
        # tentar parse leve; se já for string no formato, mantém
        try:
            dt_str = _dt.datetime.strptime(str(dt), "%Y-%m-%d").strftime("%Y-%m-%d")
        except Exception:
            # última tentativa: converter pandas/iso
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
    """
    Renderiza a página do Caixa 2 (transferência para Caixa 2).

    Preferencial:
        render_caixa2(state)

    Compatível:
        render_caixa2(None, caminho_banco='...', data_lanc=date|'YYYY-MM-DD')
    """
    # resolve inputs (compat + validação)
    try:
        _db_path, _data_lanc = _coalesce_state(state, caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Configuração incompleta: {e}")
        return

    # Toggle do formulário (mesmo comportamento do original)
    if st.button("🔄 Caixa 2", use_container_width=True, key="btn_caixa2_toggle"):
        toggle_form()

    if not form_visivel():
        return

    form = render_form()
    if not form.get("submit"):
        return

    # Validação equivalente ao original
    try:
        v = float(form.get("valor", 0) or 0)
    except Exception:
        v = 0.0

    if v <= 0:
        st.warning("⚠️ Valor inválido.")
        return

    try:
        res = transferir_para_caixa2(
            caminho_banco=_db_path,
            data_lanc=_data_lanc,
            valor=v,
        )
        if isinstance(res, dict) and res.get("ok"):
            st.session_state["msg_ok"] = res.get("msg", "Transferência realizada.")
            # fecha o formulário (mantém o comportamento original)
            st.session_state.form_caixa2 = False
            st.success(res.get("msg", "Transferência realizada com sucesso."))
            st.rerun()
        else:
            # resposta inesperada
            msg = (res or {}).get("msg") if isinstance(res, dict) else str(res)
            st.warning(f"⚠️ Não foi possível confirmar a operação. {msg or ''}".strip())
    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao transferir: {e}")
