# ===================== Page: Transferência =====================
"""
Página principal da Transferência – monta layout e chama forms/actions.

Comportamento alinhado aos outros módulos:
- Toggle do formulário
- Confirmação obrigatória
- Botão salvar independente
- Mensagens de sucesso/erro
- st.rerun() após sucesso
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Optional, Tuple

import streamlit as st

from utils.utils import coerce_data  # normaliza para datetime.date

from .state_transferencia import form_visivel, invalidate_confirm, toggle_form
from .ui_forms_transferencia import render_form_transferencia
from .actions_transferencia import carregar_nomes_bancos, registrar_transferencia_bancaria

__all__ = ["render_transferencia"]

# ===================== Constantes (session keys / ui) =====================
_BTN_TOGGLE_KEY = "btn_trf_toggle"
_BTN_SALVAR_KEY = "btn_salvar_transfer"
_SS_CONFIRMADA_KEY = "transferencia_confirmada"
_SS_FORM_FLAG = "form_transferencia"


# ===================== Helpers =====================
def _norm_date(d: Any) -> _dt.date:
    """Normaliza data para `datetime.date`.

    Aceita:
        - `datetime.date` / `datetime.datetime`
        - string 'YYYY-MM-DD', ISO, 'DD/MM/YYYY', 'DD-MM-YYYY'
        - None (usa a data de hoje)

    Args:
        d: Valor de data flexível.

    Returns:
        Data normalizada como `datetime.date`.
    """
    return coerce_data(d)


def _coalesce_state(
    state: Any,
    caminho_banco: Optional[str],
    data_lanc: Optional[Any],
) -> Tuple[str, _dt.date]:
    """Extrai `(db_path, data_lanc)` do `state` com fallback para argumentos.

    Procura `db_path` ou `caminho_banco`, e `data_lanc`|`data_lancamento`|`data`.

    Args:
        state: Objeto de estado com possíveis atributos.
        caminho_banco: Caminho do SQLite (fallback).
        data_lanc: Data do lançamento (fallback).

    Returns:
        Tupla `(db_path:str, data_lanc:date)`.

    Raises:
        ValueError: Se o caminho do banco não puder ser determinado.
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


def _to_float(v: Any) -> float:
    """Converte valor genérico para float seguro (0.0 em erro)."""
    try:
        return float(v or 0)
    except Exception:
        return 0.0


# ===================== Página =====================
def render_transferencia(
    state: Any = None,
    caminho_banco: Optional[str] = None,
    data_lanc: Optional[Any] = None,
) -> None:
    """Renderiza a página de Transferência.

    Preferencial:
        render_transferencia(state)

    Compatível:
        render_transferencia(None, caminho_banco='...', data_lanc=date|'YYYY-MM-DD')

    Args:
        state: Objeto com `db_path`/`caminho_banco` e `data_lanc`.
        caminho_banco: Caminho para o SQLite (fallback).
        data_lanc: Data do lançamento (fallback).
    """
    # Resolver entradas
    try:
        _db_path, _data_lanc = _coalesce_state(state, caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Configuração incompleta: {e}")
        return

    # Toggle do formulário
    if st.button("🔁 Transferência entre Bancos", use_container_width=True, key=_BTN_TOGGLE_KEY):
        toggle_form()

    if not form_visivel():
        return

    # Carregar bancos e renderizar form
    try:
        nomes_bancos = carregar_nomes_bancos(_db_path)
    except Exception as e:
        st.error(f"❌ Falha ao carregar bancos: {e}")
        return

    form = render_form_transferencia(_data_lanc, nomes_bancos, invalidate_confirm)

    # Confirmação obrigatória (lado servidor)
    confirmada = bool(st.session_state.get(_SS_CONFIRMADA_KEY, False))

    # Botão de salvar: desabilitado até confirmar
    save_clicked = st.button(
        "💾 Salvar Transferência",
        use_container_width=True,
        key=_BTN_SALVAR_KEY,
        disabled=not confirmada,
    )

    # Aviso persistente
    st.info("Confirme os dados para habilitar o botão de salvar.")

    # Regras de bloqueio
    if not confirmada or not save_clicked:
        return

    # ===================== Validações =====================
    orig = (form.get("banco_origem") or "").strip()
    dest = (form.get("banco_destino") or "").strip()
    valor = _to_float(form.get("valor"))

    if not orig or not dest:
        st.warning("⚠️ Informe banco de origem e destino.")
        return
    if orig == dest:
        st.warning("⚠️ Origem e destino não podem ser o mesmo banco.")
        return
    if valor <= 0:
        st.warning("⚠️ Valor inválido.")
        return

    # ===================== Execução =====================
    try:
        res = registrar_transferencia_bancaria(
            caminho_banco=_db_path,
            data_lanc=_data_lanc,
            banco_origem_in=orig,
            banco_destino_in=dest,
            valor=valor,
            observacao=form.get("observacao"),
        )

        st.session_state["msg_ok"] = res.get("msg", "Transferência registrada.")
        st.session_state[_SS_FORM_FLAG] = False  # recolhe formulário
        st.success(res.get("msg", "Transferência registrada com sucesso."))
        st.rerun()

    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao registrar transferência: {e}")
