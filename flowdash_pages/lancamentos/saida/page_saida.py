# ===================== Page: Saída =====================
"""
Página principal da Saída — monta layout e aciona forms/actions.

Comportamentos mantidos do original:
- Toggle do formulário (botão "🔴 Saída")
- Campos e fluxos idênticos (inclui Pagamentos: Fatura/Boletos/Empréstimos)
- Validações e mensagens
- `st.rerun()` após sucesso

Compatibilidade:
- Aceita `carregar_listas_para_form` como dict (novo) ou tupla/list (6/8 itens legado).
- Converte categorias/subcategorias para DataFrame com colunas ['id','nome'].
- Chama o `render_form_saida` apenas UMA vez (evita keys duplicadas tipo 'valor_saida').
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple
import datetime as _dt
import inspect
import pandas as pd
import streamlit as st

from utils.utils import coerce_data  # normaliza para datetime.date

from .state_saida import toggle_form, form_visivel, invalidate_confirm
from .ui_forms_saida import render_form_saida
from .actions_saida import (
    carregar_listas_para_form,
    registrar_saida,
)

__all__ = ["render_saida"]

# Tipo de provider
ListProvider = Callable[..., list | pd.DataFrame]


# ----------------- helpers -----------------
def _norm_date(d: Any) -> _dt.date:
    return coerce_data(d)


def _coalesce_state(
    state: Any,
    caminho_banco: Optional[str],
    data_lanc: Optional[Any],
) -> Tuple[str, _dt.date]:
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


def _as_df_id_nome(obj: Any) -> pd.DataFrame:
    """Converte entrada em DataFrame padronizado com colunas ['id','nome'].
    - Se já existir 'id', NÃO reinserimos (evita 'cannot insert id, already exists').
    - Garante presença de 'nome' (tenta renomear 1ª coluna ou 'category' -> 'nome').
    """
    if isinstance(obj, pd.DataFrame):
        df = obj.copy()
        if "nome" not in df.columns:
            if "category" in df.columns:
                df = df.rename(columns={"category": "nome"})
            elif len(df.columns) == 1:
                df.columns = ["nome"]
            elif len(df.columns) > 0:
                first = df.columns[0]
                if first != "id":
                    df = df.rename(columns={first: "nome"})
                elif len(df.columns) > 1:
                    df = df.rename(columns={df.columns[1]: "nome"})
                else:
                    df["nome"] = []
        if "id" not in df.columns:
            df.insert(0, "id", range(1, len(df) + 1))
        return df[["id", "nome"]]

    if isinstance(obj, (list, tuple, set)):
        if obj and isinstance(next(iter(obj)), dict):
            df = pd.DataFrame(obj)
            if "nome" not in df.columns:
                if "category" in df.columns:
                    df = df.rename(columns={"category": "nome"})
                elif len(df.columns) == 1:
                    df.columns = ["nome"]
                elif len(df.columns) > 0:
                    first = df.columns[0]
                    df = df.rename(columns={first: "nome"})
                else:
                    df["nome"] = []
            if "id" not in df.columns:
                df.insert(0, "id", range(1, len(df) + 1))
            return df[["id", "nome"]]
        df = pd.DataFrame({"nome": [str(x) for x in obj]})
        df.insert(0, "id", range(1, len(df) + 1))
        return df[["id", "nome"]]

    if obj is None:
        return pd.DataFrame(columns=["id", "nome"])
    df = pd.DataFrame({"nome": [str(obj)]})
    df.insert(0, "id", [1])
    return df[["id", "nome"]]


def _wrap_provider_df(provider: ListProvider) -> Callable[..., pd.DataFrame]:
    """Envolve provider (que pode aceitar args/kwargs) e sempre retorna DataFrame ['id','nome']."""
    def _wrapped(*args, **kwargs) -> pd.DataFrame:
        try:
            raw = provider(*args, **kwargs)  # provider que aceita args (ex.: categoria_id)
        except TypeError:
            raw = provider()                 # provider sem args
        return _as_df_id_nome(raw)
    return _wrapped


def _get_nome_por_id(df: pd.DataFrame, _id: Any) -> Optional[str]:
    """Busca o nome pelo id em um DF ['id','nome']."""
    if df is None or _id in (None, "", 0, "0"):
        return None
    try:
        _id = int(_id)
    except Exception:
        return None
    try:
        row = df.loc[df["id"] == _id]
        if not row.empty:
            return str(row.iloc[0]["nome"])
    except Exception:
        pass
    return None


def _payload_to_kwargs(
    payload: Dict[str, Any],
    data_lanc: _dt.date,
    usuario_nome: str,
    categorias_df: pd.DataFrame,
    listar_subcategorias_fn: Callable[..., pd.DataFrame],
) -> Dict[str, Any]:
    """Traduz o payload do form para o formato aceito por `registrar_saida`/action."""
    # Campos base
    valor = payload.get("valor_saida") or payload.get("valor") or 0
    forma = payload.get("forma_pagamento_sel") or payload.get("forma")
    origem = payload.get("origem_dinheiro_sel") or payload.get("origem")
    banco = payload.get("banco_sel") or payload.get("banco")
    descricao = payload.get("descricao") or payload.get("observacao") or payload.get("obs")
    juros = payload.get("juros") or 0
    multa = payload.get("multa") or 0
    desconto = payload.get("desconto") or 0
    trans_uid = payload.get("trans_uid")

    # Categoria/subcategoria — tentar por nome direto; se vier id, resolver nome pelo DF/provider
    categoria_nome = (
        payload.get("categoria_nome")
        or payload.get("categoria")
        or payload.get("categoria_sel_nome")
    )
    if not categoria_nome:
        cat_id = payload.get("categoria_id") or payload.get("categoria_sel") or payload.get("categoria_sel_id")
        categoria_nome = _get_nome_por_id(categorias_df, cat_id)

    subcategoria_nome = (
        payload.get("subcategoria_nome")
        or payload.get("subcategoria")
        or payload.get("subcategoria_sel_nome")
    )
    if not subcategoria_nome:
        sub_id = payload.get("subcategoria_id") or payload.get("subcategoria_sel") or payload.get("subcategoria_sel_id")
        # Se temos categoria_id, dá para montar DF de subcats certas e resolver nome por id
        cat_id = payload.get("categoria_id") or payload.get("categoria_sel") or payload.get("categoria_sel_id")
        try:
            cat_id_int = int(cat_id) if cat_id not in (None, "", 0, "0") else None
        except Exception:
            cat_id_int = None
        subcats_df = listar_subcategorias_fn(cat_id_int) if cat_id_int else pd.DataFrame(columns=["id", "nome"])
        subcategoria_nome = _get_nome_por_id(subcats_df, sub_id)

    # Integração CAP (Pagamentos)
    tipo_pag = (payload.get("tipo_pagamento_sel") or "").strip()
    tipo_obrigacao = None
    obrigacao_id = None

    if payload.get("is_pagamentos"):
        # mapear para nomenclatura esperada pela action
        mapa = {
            "Fatura Cartão de Crédito": "FATURA_CARTAO",
            "Boletos": "BOLETO",
            "Empréstimos": "EMPRESTIMO",
            "Emprestimos": "EMPRESTIMO",
        }
        tipo_obrigacao = mapa.get(tipo_pag) or None
        # tentar capturar um id de destino/obrigação
        obrigacao_id = (
            payload.get("destino_pagamento_id")
            or payload.get("obrigacao_id")
            or payload.get("id_destino_pagamento")
            or payload.get("id_obrigacao")
        )
        # coerção simples
        try:
            obrigacao_id = int(obrigacao_id) if obrigacao_id not in (None, "", 0, "0") else None
        except Exception:
            obrigacao_id = None

    return {
        "caminho_banco": payload.get("caminho_banco") or "",  # será sobrescrito pelo chamador
        "valor": valor,
        "forma": forma,
        "origem": origem,
        "banco": banco,
        "categoria": categoria_nome,
        "subcategoria": subcategoria_nome,
        "descricao": descricao,
        "usuario": usuario_nome,
        "data": data_lanc.strftime("%Y-%m-%d"),
        "juros": juros,
        "multa": multa,
        "desconto": desconto,
        "trans_uid": trans_uid,
        "tipo_obrigacao": tipo_obrigacao,
        "obrigacao_id": obrigacao_id,
    }


# ----------------- página -----------------
def render_saida(
    state: Any = None,
    caminho_banco: Optional[str] = None,
    data_lanc: Optional[Any] = None,
) -> None:
    """Renderiza a página de Saída (compat antiga e nova)."""
    # Entradas
    try:
        _db_path, _data_lanc = _coalesce_state(state, caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Configuração incompleta: {e}")
        return

    # Toggle
    if st.button("🔴 Saída", use_container_width=True, key="btn_saida_toggle"):
        toggle_form()
    if not form_visivel():
        return

    # Usuário
    usuario: Dict[str, Any] = st.session_state.get("usuario_logado", {"nome": "Sistema"})
    usuario_nome: str = usuario.get("nome", "Sistema")

    # Carregar listas (compat dict/6/8) e padronizar para DF quando necessário
    try:
        carregado = carregar_listas_para_form(_db_path)

        if isinstance(carregado, dict):
            # Dict (implementação atual de actions_saida)
            nomes_bancos = list(carregado.get("bancos", []))
            nomes_cartoes = list(carregado.get("cartoes", []))
            categorias_df = _as_df_id_nome(carregado.get("categorias", []))

            # provider RELACIONAL (categoria_id -> subcategorias)
            subprov = carregado.get("listar_subcategorias_por_categoria")
            if not callable(subprov):
                subprov = lambda categoria_id=None: []
            listar_subcategorias_fn = _wrap_provider_df(subprov)  # aceita *args/**kwargs

            # demais providers herdados (legado)
            listar_destinos_fatura_em_aberto_fn = lambda: []
            carregar_opcoes_pagamentos_fn = lambda: []
            listar_boletos_em_aberto_fn = lambda: []
            listar_empfin_em_aberto_fn = lambda: []

        elif isinstance(carregado, (list, tuple)) and len(carregado) >= 8:
            # Formato novo (8 itens)
            (
                nomes_bancos,
                nomes_cartoes,
                categorias_df,
                listar_subcategorias_fn,
                listar_destinos_fatura_em_aberto_fn,
                carregar_opcoes_pagamentos_fn,
                listar_boletos_em_aberto_fn,
                listar_empfin_em_aberto_fn,
            ) = carregado[:8]
            categorias_df = _as_df_id_nome(categorias_df)
            listar_subcategorias_fn = _wrap_provider_df(listar_subcategorias_fn)

        else:
            # Formato antigo (6 itens)
            (
                nomes_bancos,
                nomes_cartoes,
                categorias_df,
                listar_subcategorias_fn,
                listar_destinos_fatura_em_aberto_fn,
                carregar_opcoes_pagamentos_fn,
            ) = carregado[:6]
            categorias_df = _as_df_id_nome(categorias_df)
            listar_subcategorias_fn = _wrap_provider_df(listar_subcategorias_fn)
            listar_boletos_em_aberto_fn = lambda: []
            listar_empfin_em_aberto_fn = lambda: []

    except Exception as e:
        st.error(f"❌ Falha ao preparar formulário: {e}")
        return

    # Render UI (chama só UMA vez — evita duplicar widgets e keys)
    def _render_form_saida_compat() -> Dict[str, Any]:
        sig = inspect.signature(render_form_saida)
        params = sig.parameters

        kw = dict(
            data_lanc=_data_lanc,
            invalidate_cb=invalidate_confirm,
            nomes_bancos=nomes_bancos,
            nomes_cartoes=nomes_cartoes,
            categorias_df=categorias_df,                      # DF ['id','nome']
            listar_subcategorias_fn=listar_subcategorias_fn,  # provider -> DF ['id','nome']
            listar_destinos_fatura_em_aberto_fn=listar_destinos_fatura_em_aberto_fn,
            carregar_opcoes_pagamentos_fn=carregar_opcoes_pagamentos_fn,
        )
        if "listar_boletos_em_aberto_fn" in params:
            kw["listar_boletos_em_aberto_fn"] = listar_boletos_em_aberto_fn
        if "listar_empfin_em_aberto_fn" in params:
            kw["listar_empfin_em_aberto_fn"] = listar_empfin_em_aberto_fn
        return render_form_saida(**kw)

    try:
        payload: Dict[str, Any] = _render_form_saida_compat()
    except Exception as e:
        st.error(f"❌ Falha ao renderizar formulário: {e}")
        return

    # Botão salvar
    save_disabled = not st.session_state.get("confirmar_saida", False)
    if not st.button("💾 Salvar Saída", use_container_width=True, key="btn_salvar_saida", disabled=save_disabled):
        return

    if not st.session_state.get("confirmar_saida", False):
        st.warning("⚠️ Confirme os dados antes de salvar.")
        return

    # Execução com rastreio
    from shared.debug_trace import debug_wrap_ctx
    with debug_wrap_ctx("Salvar Saída"):
        try:
            # Traduz o payload para os kwargs esperados pela action
            kw = _payload_to_kwargs(
                payload=payload,
                data_lanc=_data_lanc,
                usuario_nome=usuario_nome,
                categorias_df=categorias_df,
                listar_subcategorias_fn=listar_subcategorias_fn,
            )
            kw["caminho_banco"] = _db_path  # garante caminho do banco correto

            res = registrar_saida(**kw)

        except ValueError:
            st.warning("Valor do pagamento maior que valor da fatura.")
            return
        except Exception as e:
            st.error("Erro ao registrar a saída.")
            st.exception(e)
            return

        msg_ok = res.get("mensagem", "") if isinstance(res, dict) else str(res)
        if isinstance(res, dict) and res.get("ok") and not msg_ok:
            msg_ok = "Saída registrada com sucesso."

        if isinstance(payload, dict) and payload.get("is_pagamentos") and payload.get("tipo_pagamento_sel") == "Fatura Cartão de Crédito":
            if "idempotência" not in str(msg_ok) and "já registrada" not in str(msg_ok):
                valor_pag = float(payload.get("valor_saida") or 0.0)
                msg_ok = f"Pagamento de fatura registrado! Pago: R$ {valor_pag:.2f}"

        st.session_state["msg_ok"] = msg_ok

        if isinstance(payload, dict) and payload.get("is_pagamentos") and payload.get("tipo_pagamento_sel") != "Boletos":
            st.info(
                f"Destino classificado: {payload.get('tipo_pagamento_sel')} → "
                f"{payload.get('destino_pagamento_sel') or '—'}"
            )

        st.session_state.form_saida = False
        st.success(msg_ok)
        st.rerun()
