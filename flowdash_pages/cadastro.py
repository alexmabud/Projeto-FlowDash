import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
from utils.utils import gerar_hash_senha, senha_forte, formatar_valor, formatar_percentual



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


# === Página de Cadastro de Metas =========================================================================================

# Constante global com os dias da semana
DIAS_SEMANA = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]

class MetaManager:
    def __init__(self, caminho_banco):
        self.caminho_banco = caminho_banco

    def carregar_usuarios_ativos(self):
        with sqlite3.connect(self.caminho_banco) as conn:
            df = conn.execute("SELECT id, nome FROM usuarios WHERE ativo = 1").fetchall()
            return [("LOJA", 0)] + [(nome, id) for id, nome in df]

    def salvar_meta(self, id_usuario, vendedor, mensal, semanal_percentual, dias_percentuais, perc_bronze, perc_prata, mes):
        with sqlite3.connect(self.caminho_banco) as conn:
            cursor = conn.execute("SELECT 1 FROM metas WHERE id_usuario = ? AND mes = ?", (id_usuario, mes))
            existe = cursor.fetchone()

            if existe:
                conn.execute("""UPDATE metas SET 
                    vendedor = ?, perc_segunda = ?, perc_terca = ?, perc_quarta = ?, perc_quinta = ?, perc_sexta = ?, 
                    perc_sabado = ?, perc_domingo = ?, perc_semanal = ?, meta_mensal = ?, perc_bronze = ?, perc_prata = ?
                    WHERE id_usuario = ? AND mes = ?""",
                    (vendedor.upper(), *dias_percentuais, semanal_percentual, mensal, perc_bronze, perc_prata, id_usuario, mes)
                )
            else:
                cursor = conn.execute("""SELECT perc_segunda, perc_terca, perc_quarta, perc_quinta, perc_sexta, perc_sabado, perc_domingo,
                                                perc_semanal, meta_mensal, perc_bronze, perc_prata
                                         FROM metas WHERE id_usuario = ? AND mes < ? ORDER BY mes DESC LIMIT 1""",
                                      (id_usuario, mes))
                meta_anterior = cursor.fetchone()
                if meta_anterior:
                    conn.execute("""INSERT INTO metas (
                        id_usuario, vendedor, perc_segunda, perc_terca, perc_quarta, perc_quinta, perc_sexta,
                        perc_sabado, perc_domingo, perc_semanal, meta_mensal, perc_bronze, perc_prata, mes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (id_usuario, vendedor.upper(), *meta_anterior, mes))
                else:
                    conn.execute("""INSERT INTO metas (
                        id_usuario, vendedor, perc_segunda, perc_terca, perc_quarta, perc_quinta, perc_sexta,
                        perc_sabado, perc_domingo, perc_semanal, meta_mensal, perc_bronze, perc_prata, mes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (id_usuario, vendedor.upper(), *dias_percentuais, semanal_percentual, mensal, perc_bronze, perc_prata, mes))
            conn.commit()
            return True

    def carregar_metas_cadastradas(self):
        with sqlite3.connect(self.caminho_banco) as conn:
            df = conn.execute("""SELECT COALESCE(u.nome, m.vendedor, 'LOJA') AS Vendedor, m.mes,
                                        m.meta_mensal, m.perc_semanal, m.perc_prata, m.perc_bronze,
                                        m.perc_segunda, m.perc_terca, m.perc_quarta, m.perc_quinta,
                                        m.perc_sexta, m.perc_sabado, m.perc_domingo
                                 FROM metas m LEFT JOIN usuarios u ON m.id_usuario = u.id
                                 ORDER BY m.mes DESC, Vendedor""").fetchall()
            colunas = ["Vendedor", "Mês", "Meta Mensal", "Meta Semanal", "% Prata", "% Bronze"] + DIAS_SEMANA
            return [dict(zip(colunas, linha)) for linha in df]

def pagina_metas_cadastro(caminho_banco: str):
    st.markdown("## 🎯 Cadastro de Metas")
    manager = MetaManager(caminho_banco)

    try:
        lista_usuarios = manager.carregar_usuarios_ativos()
    except Exception as e:
        st.error(f"Erro ao carregar usuários: {e}")
        return

    nomes = [nome for nome, _ in lista_usuarios]
    vendedor_selecionado = st.selectbox("Selecione o Vendedor ou 'LOJA'", nomes)
    id_usuario = dict(lista_usuarios)[vendedor_selecionado]
    mes_atual = datetime.today().strftime("%Y-%m")
    st.markdown(f"#### 📆 Mês Atual: `{mes_atual}`")

    st.markdown("### 💰 Meta Mensal")
    meta_mensal = st.number_input("Valor da meta mensal (R$)", min_value=0.0, step=100.0, format="%.2f")

    st.markdown("### 🧮 Metas Prata e Bronze")
    perc_prata = st.number_input("Percentual Prata (%)", 0.0, 100.0, 87.5, 0.5, format="%.1f")
    perc_bronze = st.number_input("Percentual Bronze (%)", 0.0, 100.0, 75.0, 0.5, format="%.1f")

    st.info(f"Meta Prata: {formatar_valor(meta_mensal * perc_prata / 100)}")
    st.info(f"Meta Bronze: {formatar_valor(meta_mensal * perc_bronze / 100)}")

    st.markdown("### 📅 Meta Semanal")
    semanal_percentual = st.number_input("Percentual da meta mensal para a meta semanal (%)", 0.0, 100.0, 25.0, 1.0, format="%.1f")
    meta_semanal_valor = meta_mensal * (semanal_percentual / 100)
    st.success(f"Meta Semanal: {formatar_valor(meta_semanal_valor)}")

    st.markdown("### 📆 Distribuição Diária (% da meta semanal)")
    col1, col2, col3 = st.columns(3)
    percentuais = []
    for i, dia in enumerate(DIAS_SEMANA):
        col = [col1, col2, col3][i % 3]
        with col:
            p = st.number_input(f"{dia} (%)", 0.0, 100.0, 0.0, 1.0, format="%.1f")
            percentuais.append(p)
            st.caption(f"→ {formatar_valor(meta_semanal_valor * (p / 100))}")

    if st.button("💾 Salvar Metas"):
        if round(sum(percentuais), 2) != 100.0:
            st.warning(f"A soma dos percentuais diários deve ser 100%. Está em {sum(percentuais):.2f}%")
        else:
            try:
                sucesso = manager.salvar_meta(
                    id_usuario=id_usuario,
                    vendedor=vendedor_selecionado,
                    mensal=meta_mensal,
                    semanal_percentual=semanal_percentual,
                    dias_percentuais=percentuais,
                    perc_bronze=perc_bronze,
                    perc_prata=perc_prata,
                    mes=mes_atual
                )
                if sucesso:
                    st.success("✅ Metas salvas com sucesso!")
                    st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar metas: {e}")

    st.divider()
    st.markdown("### 📋 Todas as Metas Cadastradas")
    try:
        metas = manager.carregar_metas_cadastradas()
        if metas:
            df = pd.DataFrame(metas)
            df["Meta Mensal"] = df["Meta Mensal"].apply(formatar_valor)
            df["Meta Semanal"] = df["Meta Semanal"].apply(formatar_percentual)
            df["% Prata"] = df["% Prata"].apply(formatar_percentual)
            df["% Bronze"] = df["% Bronze"].apply(formatar_percentual)
            for dia in DIAS_SEMANA:
                df[dia] = df[dia].apply(formatar_percentual)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhuma meta cadastrada ainda.")
    except Exception as e:
        st.error(f"Erro ao exibir metas: {e}")





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