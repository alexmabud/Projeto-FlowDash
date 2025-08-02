import streamlit as st
import sqlite3
from utils.utils import gerar_hash_senha


def validar_login(email: str, senha: str, caminho_banco: str) -> dict | None:
    """
    Valida o login do usuário com base no banco de dados.

    Args:
        email (str): Email informado pelo usuário.
        senha (str): Senha em texto plano.
        caminho_banco (str): Caminho absoluto para o banco de dados.

    Returns:
        dict | None: Dicionário com nome, email e perfil se válido; None se inválido.
    """
    senha_hash = gerar_hash_senha(senha)
    with sqlite3.connect(caminho_banco) as conn:
        cursor = conn.execute("""
            SELECT nome, email, perfil
            FROM usuarios
            WHERE email = ? AND senha = ? AND ativo = 1
        """, (email, senha_hash))
        resultado = cursor.fetchone()

    if resultado:
        return {"nome": resultado[0], "email": resultado[1], "perfil": resultado[2]}
    return None


def verificar_acesso(perfis_permitidos: list[str]) -> None:
    """
    Verifica se o perfil do usuário logado permite acesso à página atual.

    Args:
        perfis_permitidos (list[str]): Lista de perfis com acesso autorizado.

    Exibe alerta e interrompe o app se o acesso for negado.
    """
    usuario = st.session_state.get("usuario_logado")
    if not usuario or usuario.get("perfil") not in perfis_permitidos:
        st.warning("🚫 Acesso não autorizado.")
        st.stop()


def exibir_usuario_logado() -> None:
    """
    Exibe nome e perfil do usuário logado no topo da interface Streamlit.
    """
    usuario = st.session_state.get("usuario_logado")
    if usuario:
        st.markdown(f"👤 **{usuario['nome']}** — Perfil: `{usuario['perfil']}`")
        st.markdown("---")


def limpar_todas_as_paginas() -> None:
    """
    Limpa os estados de exibição das páginas no session_state.
    Usado ao alternar de módulo no menu.
    """
    chaves = [
        "mostrar_metas", "mostrar_entradas", "mostrar_saidas", "mostrar_lancamentos_do_dia",
        "mostrar_mercadorias", "mostrar_cartao_credito", "mostrar_emprestimos_financiamentos",
        "mostrar_contas_pagar", "mostrar_taxas_maquinas", "mostrar_usuarios",
        "mostrar_fechamento_caixa", "mostrar_correcao_caixa", "mostrar_cadastrar_cartao",
        "mostrar_saldos_bancarios", "mostrar_cadastro_caixa", "mostrar_cadastro_meta"
    ]
    for chave in chaves:
        st.session_state[chave] = False