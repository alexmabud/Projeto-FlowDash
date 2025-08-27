# ===================== Page: Depósito =====================
"""
Página principal do Depósito – monta layout e chama forms/actions.

Comportamento alinhado à Transferência:
- Toggle do formulário
- Confirmação obrigatória
- Botão salvar desabilitado até confirmar
- Mensagens de sucesso/erro
- st.rerun() após sucesso
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Optional, Tuple

import streamlit as st

from utils.utils import coerce_data  # normaliza para datetime.date
from .actions_deposito import carregar_nomes_bancos, registrar_deposito
from .state_deposito import form_visivel, invalidate_confirm, toggle_form
from .ui_forms_deposito import render_form_deposito


# --- helpers (mesmo estilo da Transferência) ---
def _norm_date(d: Any) -> _dt.date:
    """
    Converte a entrada em uma data (`datetime.date`).

    Args:
        d (Any): Data em vários formatos aceitos (date/datetime/str/None).

    Returns:
        datetime.date: Data normalizada.

    Raises:
        ValueError: Se a data não puder ser normalizada.
    """
    return coerce_data(d)


def _coalesce_state(
    state: Any,
    caminho_banco: Optional[str],
    data_lanc: Optional[Any],
) -> Tuple[str, _dt.date]:
    """
    Extrai `(db_path, data_lanc)` a partir do `state` com fallback para os argumentos diretos.

    Args:
        state (Any): Objeto de estado com possíveis atributos (`db_path`, `caminho_banco`, `data_lanc`, etc.).
        caminho_banco (Optional[str]): Caminho do SQLite (fallback).
        data_lanc (Optional[Any]): Data do lançamento (fallback).

    Returns:
        tuple[str, datetime.date]: Caminho do banco e a data de lançamento normalizada.

    Raises:
        ValueError: Se o caminho do banco não for informado.
    """
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
    return str(db), _norm_date(dt)


def _resolve_usuario(state: Any = None) -> str:
    """
    Obtém o usuário logado de `st.session_state` e, se não encontrado, do `state`.

    Busca nas chaves/atributos: 'usuario_logado', 'usuario', 'user_name', 'username',
    'nome_usuario', 'user', 'current_user', 'email'. Se vier dict, tenta subchaves
    ('nome', 'name', 'login', 'email').

    Args:
        state (Any, optional): Objeto de estado com possíveis atributos de usuário.

    Returns:
        str: Nome do usuário, ou string vazia se não encontrado.
    """
    # session_state
    for key in ["usuario_logado", "usuario", "user_name", "username", "nome_usuario", "user", "current_user"]:
        val = st.session_state.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
        if isinstance(val, dict):
            for subk in ("nome", "name", "login", "email"):
                v2 = val.get(subk)
                if isinstance(v2, str) and v2.strip():
                    return v2.strip()
    # state (atributos)
    if state is not None:
        for attr in ["usuario_logado", "usuario", "user_name", "username", "nome_usuario", "user", "current_user", "email"]:
            v = getattr(state, attr, None)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""


def render_deposito(
    state: Any = None,
    caminho_banco: Optional[str] = None,
    data_lanc: Optional[Any] = None,
) -> None:
    """
    Renderiza a página de Depósito. Preferencialmente chame com `render_deposito(state)`;
    é compatível com a chamada via argumentos diretos.

    Args:
        state (Any, optional): Objeto de estado com configurações e dados do contexto.
        caminho_banco (Optional[str], optional): Caminho do banco de dados SQLite.
        data_lanc (Optional[Any], optional): Data do lançamento (formatos aceitos por `coerce_data`).

    Returns:
        None
    """
    # Resolver entradas (mesma ideia da Transferência)
    try:
        _db_path, _data_lanc = _coalesce_state(state, caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Configuração incompleta: {e}")
        return

    # Toggle do formulário
    if st.button("🏦 Depósito (Caixa 2 → Banco)", use_container_width=True, key="btn_dep_toggle"):
        toggle_form()

    if not form_visivel():
        return

    # Carrega bancos e renderiza form
    try:
        nomes_bancos = carregar_nomes_bancos(_db_path)
    except Exception as e:
        st.error(f"❌ Falha ao carregar bancos: {e}")
        return

    form = render_form_deposito(_data_lanc, nomes_bancos, invalidate_confirm)

    # Confirmação obrigatória (lado servidor)
    confirmada = bool(st.session_state.get("deposito_confirmado", False))

    # Botão de salvar: desabilitado até confirmar
    save_clicked = st.button(
        "💾 Salvar Depósito",
        use_container_width=True,
        key="btn_salvar_deposito",
        disabled=not confirmada,
    )

    # Mensagem de instrução SEMPRE visível abaixo do botão
    st.info("Confirme os dados para habilitar o botão de salvar.")

    # Só continua se clicou em salvar **e** já está confirmado
    if not (confirmada and save_clicked):
        return

    # ===================== Validações =====================
    banco_dest = (form.get("banco_destino") or "").strip()
    try:
        valor = float(form.get("valor", 0) or 0)
    except Exception:
        valor = 0.0

    if not banco_dest:
        st.info("Informe o banco de destino.")
        return
    if valor <= 0:
        st.info("Valor inválido.")
        return

    # ===================== Execução =====================
    try:
        usuario_atual = _resolve_usuario(state)
        res = registrar_deposito(
            caminho_banco=_db_path,
            data_lanc=_data_lanc,
            valor=valor,
            banco_in=banco_dest,
            usuario=usuario_atual,
        )
        # Banner de sucesso padrão (página principal lê msg_ok)
        st.session_state["msg_ok"] = res.get("msg", "Depósito registrado.")
        st.session_state.form_deposito = False
        st.success(res.get("msg", "Depósito registrado com sucesso."))
        st.rerun()
    except ValueError as ve:
        st.info(f"{ve}")
    except Exception as e:
        st.error(f"❌ Erro ao registrar depósito: {e}")
