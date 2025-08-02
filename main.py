"""
FlowDash — Arquivo principal da aplicação Streamlit

Este script é o ponto de entrada do sistema FlowDash. Ele é responsável por:

- Realizar o login e validação de usuários via banco SQLite
- Controlar o menu lateral com base no perfil do usuário (Administrador, Gerente, Vendedor)
- Carregar dinamicamente as páginas do sistema conforme a opção selecionada
- Exibir o nome do usuário logado e controlar o logout
- Gerenciar o session_state para navegação e controle de interface

Módulos carregados de forma dinâmica:
- pages.metas
- pages.lancamentos
- pages.dataframes
- pages.fechamento
- pages.dashboard
- pages.dre
- pages.cadastro
"""


import streamlit as st
from auth.auth import validar_login, verificar_acesso, exibir_usuario_logado, limpar_todas_as_paginas

st.set_page_config(page_title="FlowDash", layout="wide")

# Caminho do banco de dados
caminho_banco = "data/flowdash_data.db"

# Inicializa sessão
if "usuario_logado" not in st.session_state:
    st.session_state.usuario_logado = None

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
                st.rerun()
            else:
                st.error("❌ Email ou senha inválidos, ou usuário inativo.")
    st.stop()

# ===== SIDEBAR E MENU ============================================================================
usuario = st.session_state.usuario_logado
perfil = usuario["perfil"]

st.sidebar.markdown(f"👤 **{usuario['nome']}**\n🔐 Perfil: `{perfil}`")
if st.sidebar.button("🚪 Sair"):
    st.session_state.usuario_logado = None
    st.rerun()

st.sidebar.markdown("___")
st.sidebar.markdown("### 🧭 Menu de Navegação")

# Define opções por perfil
menu = []

if perfil in ["Administrador", "Gerente", "Vendedor"]:
    menu.append("🎯 Metas")
    menu.append("🧾 Lançamentos")
    menu.append("📋 DataFrames")

if perfil in ["Administrador", "Gerente"]:
    menu.append("💼 Fechamento de Caixa")
    menu.append("📊 Dashboard")
    menu.append("📉 DRE")

if perfil == "Administrador":
    menu.append("🛠️ Cadastro")

opcao = st.sidebar.radio("Selecione uma opção:", menu)

# Limpa a tela ao trocar
if "pagina_atual" not in st.session_state or st.session_state.pagina_atual != opcao:
    limpar_todas_as_paginas()
    st.session_state.pagina_atual = opcao
    st.rerun()

# ===== TÍTULO PRINCIPAL =========================================================================
st.title(opcao)
exibir_usuario_logado()

# ===== Roteamento para páginas externas =========================================================
if opcao == "🎯 Metas":
    from pages.metas import pagina_metas
    pagina_metas(caminho_banco)

elif opcao == "🧾 Lançamentos":
    from pages.lancamentos import pagina_lancamentos
    pagina_lancamentos(caminho_banco)

elif opcao == "💼 Fechamento de Caixa":
    from pages.fechamento import pagina_fechamento
    pagina_fechamento(caminho_banco)

elif opcao == "📋 DataFrames":
    from pages.dataframes import pagina_dataframes
    pagina_dataframes(caminho_banco)

elif opcao == "📊 Dashboard":
    from pages.dashboard import pagina_dashboard
    pagina_dashboard(caminho_banco)

elif opcao == "📉 DRE":
    from pages.dre import pagina_dre
    pagina_dre(caminho_banco)

elif opcao == "🛠️ Cadastro":
    from pages.cadastro import pagina_cadastro
    pagina_cadastro(caminho_banco)