# ===================== Page: Depósito =====================
"""
Página principal do Depósito – monta layout e chama forms/actions.
Preserva o comportamento do arquivo original: toggle, confirmação, validações,
mensagens e rerun após sucesso.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Optional

import streamlit as st

from .state_deposito import toggle_form, form_visivel
from .ui_forms_deposito import render_form
from .actions_deposito import registrar_deposito, carregar_nomes_bancos

__all__ = ["render_deposito"]


def _coalesce_state(
    state: Any,
    caminho_banco: Optional[str],
    data_lanc: Optional[Any],
) -> tuple[str, str]:
    """Extrai (db_path, data_lanc YYYY-MM-DD) do `state` com fallback para args diretos."""
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


def render_deposito(
    state: Any = None,
    caminho_banco: Optional[str] = None,
    data_lanc: Optional[Any] = None,
) -> None:
    """
    Renderiza a página de Depósito.

    Preferencial:
        render_deposito(state)

    Compatível:
        render_deposito(None, caminho_banco='...', data_lanc=date|'YYYY-MM-DD')
    """
    # Resolver entradas
    try:
        _db_path, _data_lanc = _coalesce_state(state, caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Configuração incompleta: {e}")
        return

    # Toggle do formulário
    if st.button("🏦 Depósito Bancário", use_container_width=True, key="btn_deposito_toggle"):
        toggle_form()

    if not form_visivel():
        return

    # Carrega bancos e renderiza form
    try:
        nomes_bancos = carregar_nomes_bancos(_db_path)
    except Exception as e:
        st.error(f"❌ Falha ao carregar bancos: {e}")
        return

    form = render_form(_data_lanc, nomes_bancos)
    if not form.get("submit"):
        return

    # Trava extra de confirmação (lado servidor)
    if not st.session_state.get("deposito_confirmar", False):
        st.warning("⚠️ Confirme os dados antes de salvar.")
        return

    # Validações
    try:
        valor = float(form.get("valor", 0) or 0)
    except Exception:
        valor = 0.0
    if valor <= 0:
        st.warning("⚠️ Valor inválido.")
        return

    banco_escolhido = (form.get("banco_escolhido") or "").strip()
    if not banco_escolhido:
        st.warning("⚠️ Selecione ou digite o banco de destino.")
        return

    # Execução
    try:
        res = registrar_deposito(
            caminho_banco=_db_path,
            data_lanc=_data_lanc,
            valor=valor,
            banco_in=banco_escolhido,
        )
        st.session_state["msg_ok"] = res.get("msg", "Depósito registrado.")
        st.session_state.form_deposito = False
        st.success(res.get("msg", "Depósito registrado com sucesso."))
        st.rerun()
    except RuntimeError as warn:  # avisos de upsert em saldos_bancos
        st.warning(str(warn))
        st.session_state.form_deposito = False
        st.rerun()
    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao registrar depósito: {e}")
