import streamlit as st
import sqlite3
import pandas as pd
from utils.utils import gerar_hash_senha, senha_forte


# === Pagina de Cadastro de Usuários =========================================================================================
def pagina_usuarios(caminho_banco: str):
    class Usuario:
        def __init__(self, id, nome, email, perfil, ativo):
            self.id = id
            self.nome = nome
            self.email = email
            self.perfil = perfil
            self.ativo = ativo

        def exibir_info(self):
            status = "🟢 Ativo" if self.ativo == 1 else "🔴 Inativo"
            return self.nome, self.email, status

        def alternar_status(self, caminho_banco):
            novo_status = 0 if self.ativo == 1 else 1
            with sqlite3.connect(caminho_banco) as conn:
                conn.execute("UPDATE usuarios SET ativo = ? WHERE id = ?", (novo_status, self.id))
                conn.commit()

        def excluir(self, caminho_banco):
            with sqlite3.connect(caminho_banco) as conn:
                conn.execute("DELETE FROM usuarios WHERE id = ?", (self.id,))
                conn.commit()

    st.subheader("👥 Cadastro de Usuários")

    with st.form("form_usuarios"):
        col1, col2 = st.columns(2)

        with col1:
            nome = st.text_input("Nome Completo", max_chars=100)
            perfil = st.selectbox("Perfil", ["Administrador", "Gerente", "Vendedor"])

        with col2:
            email = st.text_input("Email", max_chars=100)
            ativo = st.selectbox("Usuário Ativo?", ["Sim", "Não"])

        senha = st.text_input("Senha", type="password", max_chars=50)
        confirmar_senha = st.text_input("Confirmar Senha", type="password", max_chars=50)

        submitted = st.form_submit_button("💾 Salvar Usuário")

        if submitted:
            if not nome or not email or not senha or not confirmar_senha:
                st.error("❗ Todos os campos são obrigatórios!")
            elif senha != confirmar_senha:
                st.warning("⚠️ As senhas não coincidem. Tente novamente.")
            elif not senha_forte(senha):
                st.warning("⚠️ A senha deve ter pelo menos 8 caracteres, com letra maiúscula, minúscula, número e símbolo.")
            elif "@" not in email or "." not in email:
                st.warning("⚠️ Digite um e-mail válido.")
            else:
                senha_hash = gerar_hash_senha(senha)
                ativo_valor = 1 if ativo == "Sim" else 0
                try:
                    with sqlite3.connect(caminho_banco) as conn:
                        conn.execute("""
                            INSERT INTO usuarios (nome, email, senha, perfil, ativo)
                            VALUES (?, ?, ?, ?, ?)
                        """, (nome, email, senha_hash, perfil, ativo_valor))
                        conn.commit()
                    st.success("✅ Usuário cadastrado com sucesso!")
                    st.rerun()
                except sqlite3.IntegrityError:
                    st.error("⚠️ Email já cadastrado!")
                except Exception as e:
                    st.error(f"❌ Erro ao salvar usuário: {e}")

    st.markdown("### 📋 Usuários Cadastrados:")

    with sqlite3.connect(caminho_banco) as conn:
        df = pd.read_sql("SELECT id, nome, email, perfil, ativo FROM usuarios", conn)

    if not df.empty:
        for _, row in df.iterrows():
            usuario = Usuario(row["id"], row["nome"], row["email"], row["perfil"], row["ativo"])
            col1, col2, col3, col4, col5 = st.columns([2, 3, 2, 2, 2])

            with col1:
                st.write(f"👤 {usuario.nome}")
            with col2:
                st.write(usuario.email)
            with col3:
                st.write(usuario.exibir_info()[2])
            with col4:
                if st.button("🔁 ON/OFF", key=f"ativar_{usuario.id}"):
                    usuario.alternar_status(caminho_banco)
                    st.rerun()
            with col5:
                if st.session_state.get(f"confirmar_exclusao_{usuario.id}", False):
                    st.warning(f"❓ Tem certeza que deseja excluir o usuário '{usuario.nome}'?")
                    col_c, col_d = st.columns(2)
                    with col_c:
                        if st.button("✅ Confirmar", key=f"confirma_{usuario.id}"):
                            usuario.excluir(caminho_banco)
                            st.success(f"✅ Usuário '{usuario.nome}' excluído com sucesso!")
                            st.rerun()
                    with col_d:
                        if st.button("❌ Cancelar", key=f"cancelar_{usuario.id}"):
                            st.session_state[f"confirmar_exclusao_{usuario.id}"] = False
                            st.rerun()
                else:
                    if st.button("🗑️ Excluir", key=f"excluir_{usuario.id}"):
                        if st.session_state.usuario_logado["email"] == usuario.email:
                            st.warning("⚠️ Você não pode excluir seu próprio usuário enquanto estiver logado.")
                        else:
                            st.session_state[f"confirmar_exclusao_{usuario.id}"] = True
                            st.rerun()
    else:
        st.info("ℹ️ Nenhum usuário cadastrado.")



def pagina_metas_cadastro(caminho_banco: str):
    st.subheader("🎯 Cadastro de Metas")
    st.info("🚧 Em desenvolvimento...")

def pagina_taxas_maquinas(caminho_banco: str):
    st.subheader("⚙️ Taxas Maquinetas")
    st.info("🚧 Em desenvolvimento...")

def pagina_cartoes_credito(caminho_banco: str):
    st.subheader("📇 Cartão de Crédito")
    st.info("🚧 Em desenvolvimento...")

def pagina_caixa(caminho_banco: str):
    st.subheader("💵 Caixa")
    st.info("🚧 Em desenvolvimento...")

def pagina_correcao_caixa(caminho_banco: str):
    st.subheader("🛠️ Correção de Caixa")
    st.info("🚧 Em desenvolvimento...")

def pagina_saldos_bancarios(caminho_banco: str):
    st.subheader("🏦 Saldos Bancários")
    st.info("🚧 Em desenvolvimento...")

def pagina_emprestimos_cadastro(caminho_banco: str):
    st.subheader("🏛️ Empréstimos/Financiamentos")
    st.info("🚧 Em desenvolvimento...")