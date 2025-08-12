import streamlit as st
import importlib
from auth.auth import validar_login, verificar_acesso, exibir_usuario_logado, limpar_todas_as_paginas
from utils.utils import garantir_trigger_totais_saldos_caixas

st.set_page_config(page_title="FlowDash", layout="wide")

# Caminho do banco de dados
caminho_banco = "data/flowdash_data.db"
try:
    garantir_trigger_totais_saldos_caixas(caminho_banco)
except Exception as e:
    st.warning(f"Trigger de totais não criada: {e}")

# ===== Sessão ====================================================================================
if "usuario_logado" not in st.session_state:
    st.session_state.usuario_logado = None
if "pagina_atual" not in st.session_state:
    st.session_state.pagina_atual = "📊 Dashboard"

# ===== LOGIN =====================================================================================
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
                st.session_state.pagina_atual = "📊 Dashboard" if usuario["perfil"] == "Administrador" else "🎯 Metas"
                st.rerun()
            else:
                st.error("❌ Email ou senha inválidos, ou usuário inativo.")
    st.stop()

# ===== SIDEBAR E MENU ============================================================================
usuario = st.session_state.usuario_logado
perfil = usuario["perfil"]

st.sidebar.markdown(f"👤 **{usuario['nome']}**\n🔐 Perfil: `{perfil}`")

# Logout
if st.sidebar.button("🚪 Sair", use_container_width=True):
    st.session_state.usuario_logado = None
    st.rerun()

st.sidebar.markdown("___")

# Atalho para nova venda
if st.sidebar.button("➕ Nova Venda", key="nova_venda", use_container_width=True):
    st.session_state.pagina_atual = "🧾 Lançamentos"
    st.session_state.ir_para_formulario = True
    st.rerun()

st.sidebar.markdown("___")
st.sidebar.markdown("## 🧭 Menu de Navegação")

# Menu direto (cada botão troca a página e dá rerun)
if st.sidebar.button("🎯 Metas", use_container_width=True):
    st.session_state.pagina_atual = "🎯 Metas"
    st.rerun()

if st.sidebar.button("🧾 Lançamentos", use_container_width=True):
    st.session_state.pagina_atual = "🧾 Lançamentos"
    st.rerun()

if st.sidebar.button("💼 Fechamento de Caixa", use_container_width=True):
    st.session_state.pagina_atual = "💼 Fechamento de Caixa"
    st.rerun()

if st.sidebar.button("📊 Dashboard", use_container_width=True):
    st.session_state.pagina_atual = "📊 Dashboard"
    st.rerun()

if st.sidebar.button("📉 DRE", use_container_width=True):
    st.session_state.pagina_atual = "📉 DRE"
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

# Expander: Cadastro (somente Admin)
if perfil == "Administrador":
    with st.sidebar.expander("🛠️ Cadastro", expanded=False):
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

# ===== TÍTULO PRINCIPAL ========================================================================
st.title(st.session_state.pagina_atual)

# ===== ROTEAMENTO PARA PÁGINAS =================================================================
ROTAS = {
    "🎯 Metas": "flowdash_pages.metas.pagina_metas",
    "🧾 Lançamentos": "flowdash_pages.lancamentos.pagina.pagina_lancamentos",
    "💼 Fechamento de Caixa": "flowdash_pages.fechamento.pagina_fechamento",
    "📊 Dashboard": "flowdash_pages.dashboard.pagina_dashboard",
    "📉 DRE": "flowdash_pages.dre.pagina_dre",
    "📥 Entradas": "flowdash_pages.dataframes.pagina_entradas",
    "📤 Saídas": "flowdash_pages.dataframes.pagina_saidas",
    "📦 Mercadorias": "flowdash_pages.dataframes.pagina_mercadorias",
    "💳 Fatura Cartão de Crédito": "flowdash_pages.dataframes.pagina_fatura_cartao",
    "📄 Contas a Pagar": "flowdash_pages.dataframes.pagina_contas_pagar",
    "🏦 Empréstimos/Financiamentos": "flowdash_pages.dataframes.pagina_emprestimos",
    "👥 Usuários": "flowdash_pages.cadastros.pagina_usuarios.pagina_usuarios",
    "🎯 Cadastro de Metas": "flowdash_pages.cadastros.pagina_metas.pagina_metas_cadastro",
    "⚙️ Taxas Maquinetas": "flowdash_pages.cadastros.pagina_maquinetas.pagina_taxas_maquinas",
    "📇 Cartão de Crédito": "flowdash_pages.cadastros.pagina_cartoes.pagina_cartoes_credito",
    "💵 Caixa": "flowdash_pages.cadastros.pagina_caixa.pagina_caixa",
    "🛠️ Correção de Caixa": "flowdash_pages.cadastros.pagina_correcao_caixa.pagina_correcao_caixa",
    "🏦 Saldos Bancários": "flowdash_pages.cadastros.pagina_saldos_bancarios.pagina_saldos_bancarios",
    "🏛️ Cadastro de Empréstimos": "flowdash_pages.cadastros.pagina_emprestimos.pagina_emprestimos_financiamentos",
    "🏦 Cadastro de Bancos": "flowdash_pages.cadastros.pagina_bancos_cadastrados.pagina_cadastro_bancos",
    "📂 Cadastro de Saídas": "flowdash_pages.cadastros.cadastro_categorias.pagina_cadastro_categorias",
}

pagina = st.session_state.get("pagina_atual", "📊 Dashboard")

if pagina in ROTAS:
    limpar_todas_as_paginas()
    modulo_nome, funcao_nome = ROTAS[pagina].rsplit(".", 1)
    try:
        modulo = importlib.import_module(modulo_nome)
        pagina_func = getattr(modulo, funcao_nome)
    except Exception as e:
        st.error(f"Falha ao carregar página '{pagina}': {e}")
    else:
        pagina_func(caminho_banco)
else:
    st.warning("Página não encontrada.")