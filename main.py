"""
FlowDash — Main App
===================

Ponto de entrada do aplicativo Streamlit do FlowDash.
"""

from __future__ import annotations
import os
import importlib
import inspect
import streamlit as st

from auth.auth import (
    validar_login,
    verificar_acesso,           # disponível para uso dentro das páginas
    exibir_usuario_logado,      # disponível para uso dentro das páginas
    limpar_todas_as_paginas,
)
from utils.utils import garantir_trigger_totais_saldos_caixas


# ======================================================================================
# Configuração inicial da página
# ======================================================================================
st.set_page_config(page_title="FlowDash", layout="wide")

# Caminho do banco de dados
caminho_banco = os.path.join("data", "flowdash_data.db")
os.makedirs("data", exist_ok=True)

# Infra mínima de BD (idempotente)
try:
    garantir_trigger_totais_saldos_caixas(caminho_banco)
except Exception as e:
    st.warning(f"Trigger de totais não criada: {e}")


# ======================================================================================
# Estado de sessão
# ======================================================================================
if "usuario_logado" not in st.session_state:
    st.session_state.usuario_logado = None
if "pagina_atual" not in st.session_state:
    st.session_state.pagina_atual = "📊 Dashboard"


# ======================================================================================
# Helper de roteamento — importa módulo e chama render/page/main (+ fallbacks)
# ======================================================================================
def _call_page(module_path: str):
    """
    Importa o módulo indicado e tenta chamar, nesta ordem, a função:

    - genéricas: render, page, main, pagina, show, pagina_fechamento_caixa
    - derivadas do nome do arquivo: render_<tail>, render_page, render_<seg>, render_<parent>,
      page_<tail>, show_<tail>, <seg> (se for callable)
    - fallbacks: 1ª função que comece com 'pagina_' ou 'render_'

    Suporta parâmetros:
      - sempre fornece 'caminho_banco' se a função aceitar;
      - para outros parâmetros OBRIGATÓRIOS, usa valores do session_state se existirem;
        caso contrário preenche com None. Se o parâmetro for posicional/aceitar posicional,
        passamos como **posicional** para evitar erro do tipo “missing required positional argument”.
    """
    try:
        mod = importlib.import_module(module_path)
    except Exception as e:
        st.error(f"Falha ao importar módulo '{module_path}': {e}")
        return

    def _invoke(fn):
        sig = inspect.signature(fn)
        args = []
        kwargs = {}

        ss = st.session_state
        usuario_logado = ss.get("usuario_logado")
        known = {
            "usuario": usuario_logado,
            "usuario_logado": usuario_logado,
            "perfil": (usuario_logado or {}).get("perfil") if usuario_logado else None,
            "pagina_atual": ss.get("pagina_atual"),
            "ir_para_formulario": ss.get("ir_para_formulario"),
            "caminho_banco": caminho_banco,
        }

        # Construção obedecendo a ordem dos parâmetros
        # Regra:
        # - Se for 'caminho_banco', colocamos **posicional** (para não depender de keywords).
        # - Se for obrigatório sem default e não houver valor no estado/conhecidos -> passamos None.
        #   * Se o parâmetro for POSITIONAL_ONLY ou POSITIONAL_OR_KEYWORD -> vai em args (posicional).
        #   * Se for KEYWORD_ONLY -> vai em kwargs.
        for p in sig.parameters.values():
            name = p.name
            kind = p.kind
            has_default = (p.default is not inspect._empty)

            # 1) caminho_banco sempre fornecido
            if name == "caminho_banco":
                args.append(caminho_banco)
                continue

            # 2) valor conhecido/estado?
            if name in known:
                val = known[name]
                # preferir posicional quando o parâmetro aceita posicional
                if kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                    args.append(val)
                else:
                    kwargs[name] = val
                continue
            if name in ss:
                val = ss[name]
                if kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                    args.append(val)
                else:
                    kwargs[name] = val
                continue

            # 3) obrigatório sem default -> preencher com None
            if not has_default:
                if kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                    args.append(None)
                else:  # KEYWORD_ONLY
                    kwargs[name] = None
            # se tiver default e não veio de lugar nenhum, não passamos nada

        return fn(*args, **kwargs)

    seg = module_path.rsplit(".", 1)[-1]                 # ex.: 'page_venda'
    parent = module_path.rsplit(".", 2)[-2] if "." in module_path else ""
    tail = seg.split("_", 1)[1] if "_" in seg else seg   # ex.: 'venda'

    base = ["render", "page", "main", "pagina", "show", "pagina_fechamento_caixa"]
    derived = [
        f"render_{tail}",
        "render_page",
        f"render_{seg}",
        f"render_{parent}",
        f"page_{tail}",
        f"show_{tail}",
        seg,  # função com o mesmo nome do módulo (ex.: page_venda)
    ]

    tried = set()
    for fn_name in base + derived:
        if fn_name in tried or not hasattr(mod, fn_name):
            tried.add(fn_name)
            continue
        tried.add(fn_name)
        fn = getattr(mod, fn_name)
        if callable(fn):
            try:
                return _invoke(fn)
            except Exception as e:
                st.error(f"Erro ao executar {module_path}.{fn_name}: {e}")
                return

    # fallbacks: primeira função começando com 'pagina_' ou 'render_'
    for prefix in ("pagina_", "render_"):
        for name, obj in vars(mod).items():
            if callable(obj) and name.startswith(prefix):
                try:
                    return _invoke(obj)
                except Exception as e:
                    st.error(f"Erro ao executar {module_path}.{name}: {e}")
                    return

    st.warning(f"O módulo '{module_path}' não possui função compatível (render/page/main/pagina*/show).")


# ======================================================================================
# LOGIN
# ======================================================================================
if not st.session_state.usuario_logado:
    st.title("🔐 Login")

    with st.form("form_login"):
        email = st.text_input("Email")
        senha = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar")

        if submitted:
            usuario = validar_login(email, senha, caminho_banco)
            if usuario:
                st.session_state.usuario_logado = usuario
                # Redirecionamento inicial por perfil
                st.session_state.pagina_atual = (
                    "📊 Dashboard" if usuario["perfil"] in ("Administrador", "Gerente")
                    else "🧾 Lançamentos"
                )
                limpar_todas_as_paginas()  # limpa chaves antigas, preserva usuario_logado/pagina_atual
                st.rerun()
            else:
                st.error("❌ Email ou senha inválidos, ou usuário inativo.")
    st.stop()


# ======================================================================================
# Sidebar: usuário + navegação
# ======================================================================================
usuario = st.session_state.get("usuario_logado")
if usuario is None:
    st.warning("Faça login para continuar.")
    st.stop()

perfil = usuario["perfil"]

# Cabeçalho do usuário logado
st.sidebar.markdown(f"👤 **{usuario['nome']}**\n🔐 Perfil: `{perfil}`")

# Logout
if st.sidebar.button("🚪 Sair", use_container_width=True):
    limpar_todas_as_paginas()
    st.session_state.usuario_logado = None
    st.rerun()

st.sidebar.markdown("---")

# Atalho para nova venda (leva para Lançamentos e sinaliza formulário)
if st.sidebar.button("➕ Nova Venda", key="nova_venda", use_container_width=True):
    st.session_state.pagina_atual = "🧾 Lançamentos"
    st.session_state.ir_para_formulario = True
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("## 🧭 Menu de Navegação")

# Botões principais (alteram página e rerun)
if st.sidebar.button("📊 Dashboard", use_container_width=True):
    st.session_state.pagina_atual = "📊 Dashboard"
    st.rerun()

if st.sidebar.button("📉 DRE", use_container_width=True):
    st.session_state.pagina_atual = "📉 DRE"
    st.rerun()

if st.sidebar.button("🧾 Lançamentos", use_container_width=True):
    st.session_state.pagina_atual = "🧾 Lançamentos"
    st.rerun()

if st.sidebar.button("💼 Fechamento de Caixa", use_container_width=True):
    st.session_state.pagina_atual = "💼 Fechamento de Caixa"
    st.rerun()

if st.sidebar.button("🎯 Metas", use_container_width=True):
    st.session_state.pagina_atual = "🎯 Metas"
    st.rerun()

# Expander: DataFrames
with st.sidebar.expander("📋 DataFrames", expanded=False):
    if st.button("📥 Entradas", use_container_width=True):
        st.session_state.pagina_atual = "📥 Entradas"
        st.rerun()
    if st.button("📤 Saídas", use_container_width=True):
        st.session_state.pagina_atual = "📤 Saídas"
        st.rerun()
    if st.button("📦 Mercadorias", use_container_width=True):
        st.session_state.pagina_atual = "📦 Mercadorias"
        st.rerun()
    if st.button("💳 Fatura Cartão de Crédito", use_container_width=True):
        st.session_state.pagina_atual = "💳 Fatura Cartão de Crédito"
        st.rerun()
    if st.button("📄 Contas a Pagar", use_container_width=True):
        st.session_state.pagina_atual = "📄 Contas a Pagar"
        st.rerun()
    if st.button("🏦 Empréstimos/Financiamentos", use_container_width=True):
        st.session_state.pagina_atual = "🏦 Empréstimos/Financiamentos"
        st.rerun()

# Expander: Cadastros (apenas Administrador)
if perfil == "Administrador":
    with st.sidebar.expander("🛠️ Cadastros", expanded=False):
        if st.button("👥 Usuários", use_container_width=True):
            st.session_state.pagina_atual = "👥 Usuários"
            st.rerun()
        if st.button("🎯 Cadastro de Metas", use_container_width=True):
            st.session_state.pagina_atual = "🎯 Cadastro de Metas"
            st.rerun()
        if st.button("⚙️ Taxas Maquinetas", use_container_width=True):
            st.session_state.pagina_atual = "⚙️ Taxas Maquinetas"
            st.rerun()
        if st.button("📇 Cartão de Crédito", use_container_width=True):
            st.session_state.pagina_atual = "📇 Cartão de Crédito"
            st.rerun()
        if st.button("💵 Caixa", use_container_width=True):
            st.session_state.pagina_atual = "💵 Caixa"
            st.rerun()
        if st.button("🛠️ Correção de Caixa", use_container_width=True):
            st.session_state.pagina_atual = "🛠️ Correção de Caixa"
            st.rerun()
        if st.button("🏦 Saldos Bancários", use_container_width=True):
            st.session_state.pagina_atual = "🏦 Saldos Bancários"
            st.rerun()
        if st.button("🏛️ Cadastro de Empréstimos", use_container_width=True):
            st.session_state.pagina_atual = "🏛️ Cadastro de Empréstimos"
            st.rerun()
        if st.button("🏦 Cadastro de Bancos", use_container_width=True):
            st.session_state.pagina_atual = "🏦 Cadastro de Bancos"
            st.rerun()
        if st.button("📂 Cadastro de Saídas", use_container_width=True):
            st.session_state.pagina_atual = "📂 Cadastro de Saídas"
            st.rerun()


# ======================================================================================
# Título principal
# ======================================================================================
st.title(st.session_state.pagina_atual)


# ======================================================================================
# Roteamento (módulos compatíveis com sua árvore)
# ======================================================================================
ROTAS = {
    # páginas principais
    "📊 Dashboard": "flowdash_pages.dashboard.dashboard",
    "📉 DRE": "flowdash_pages.dre.dre",
    "🧾 Lançamentos": "flowdash_pages.lancamentos.pagina.page_lancamentos",
    "💼 Fechamento de Caixa": "flowdash_pages.fechamento.fechamento",
    "🎯 Metas": "flowdash_pages.metas.metas",

    # dataframes (um único módulo que decide internamente o que exibir)
    "📥 Entradas": "flowdash_pages.dataframes.dataframes",
    "📤 Saídas": "flowdash_pages.dataframes.dataframes",
    "📦 Mercadorias": "flowdash_pages.dataframes.dataframes",
    "💳 Fatura Cartão de Crédito": "flowdash_pages.dataframes.dataframes",
    "📄 Contas a Pagar": "flowdash_pages.dataframes.dataframes",
    "🏦 Empréstimos/Financiamentos": "flowdash_pages.dataframes.dataframes",

    # cadastros (nomes batendo com a pasta cadastros/)
    "👥 Usuários": "flowdash_pages.cadastros.pagina_usuarios",
    "🎯 Cadastro de Metas": "flowdash_pages.cadastros.pagina_metas",
    "⚙️ Taxas Maquinetas": "flowdash_pages.cadastros.pagina_maquinetas",
    "📇 Cartão de Crédito": "flowdash_pages.cadastros.pagina_cartoes",
    "💵 Caixa": "flowdash_pages.cadastros.pagina_caixa",
    "🛠️ Correção de Caixa": "flowdash_pages.cadastros.pagina_correcao_caixa",
    "🏦 Saldos Bancários": "flowdash_pages.cadastros.pagina_saldos_bancarios",
    "🏛️ Cadastro de Empréstimos": "flowdash_pages.cadastros.pagina_emprestimos",
    "🏦 Cadastro de Bancos": "flowdash_pages.cadastros.pagina_bancos_cadastrados",
    "📂 Cadastro de Saídas": "flowdash_pages.cadastros.cadastro_categorias",
}

# (opcional) controle simples de acesso por página
PERMISSOES = {
    "📊 Dashboard": {"Administrador", "Gerente"},
    "📉 DRE": {"Administrador", "Gerente"},
    "🧾 Lançamentos": {"Administrador", "Gerente", "Vendedor"},
    "💼 Fechamento de Caixa": {"Administrador", "Gerente"},
    "🎯 Metas": {"Administrador", "Gerente"},

    # dataframes e cadastros
    "📥 Entradas": {"Administrador", "Gerente"},
    "📤 Saídas": {"Administrador", "Gerente"},
    "📦 Mercadorias": {"Administrador", "Gerente"},
    "💳 Fatura Cartão de Crédito": {"Administrador", "Gerente"},
    "📄 Contas a Pagar": {"Administrador", "Gerente"},
    "🏦 Empréstimos/Financiamentos": {"Administrador", "Gerente"},
    "👥 Usuários": {"Administrador"},
    "🎯 Cadastro de Metas": {"Administrador"},
    "⚙️ Taxas Maquinetas": {"Administrador"},
    "📇 Cartão de Crédito": {"Administrador"},
    "💵 Caixa": {"Administrador"},
    "🛠️ Correção de Caixa": {"Administrador"},
    "🏦 Saldos Bancários": {"Administrador"},
    "🏛️ Cadastro de Empréstimos": {"Administrador"},
    "🏦 Cadastro de Bancos": {"Administrador"},
    "📂 Cadastro de Saídas": {"Administrador"},
}

pagina = st.session_state.get("pagina_atual", "📊 Dashboard")

if pagina in ROTAS:
    perfil_atual = st.session_state.usuario_logado["perfil"]
    if pagina in PERMISSOES and perfil_atual not in PERMISSOES[pagina]:
        st.error("Acesso negado para o seu perfil.")
    else:
        _call_page(ROTAS[pagina])
else:
    st.warning("Página não encontrada.")
