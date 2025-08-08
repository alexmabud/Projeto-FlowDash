import streamlit as st
import sqlite3
import pandas as pd
from datetime import date
from flowdash_pages.cadastros.cadastro_classes import BancoRepository

def pagina_saldos_bancarios(caminho_banco: str):
    st.subheader("🏦 Cadastro de Saldos Bancários por Banco")

    #  Exibe mensagem persistente após o rerun
    if "mensagem_sucesso" in st.session_state:
        st.success(st.session_state["mensagem_sucesso"])
        del st.session_state["mensagem_sucesso"]

    data = st.date_input("📅 Data do Saldo", value=date.today())
    data_str = str(data)

    repo_banco = BancoRepository(caminho_banco)
    df_bancos = repo_banco.carregar_bancos()

    if df_bancos.empty:
        st.warning("⚠️ Nenhum banco cadastrado. Cadastre um banco primeiro.")
        return

    bancos = df_bancos["nome"].tolist()
    banco_selecionado = st.selectbox("🏦 Selecione o banco:", bancos)
    valor_digitado = st.number_input("💰 Valor do saldo", min_value=0.0, step=10.0, format="%.2f")

    if st.button("💾 Salvar Saldo"):
        try:
            with sqlite3.connect(caminho_banco) as conn:
                # Verifica se já existe uma linha para a data
                df_existente = pd.read_sql("SELECT * FROM saldos_bancos WHERE data = ?", conn, params=(data_str,))
                if df_existente.empty:
                    # Cria um dicionário com 0 para todos os bancos
                    valores = {banco: 0.0 for banco in bancos}
                    valores[banco_selecionado] = valor_digitado
                    valores["data"] = data_str
                    pd.DataFrame([valores]).to_sql("saldos_bancos", conn, if_exists="append", index=False)
                else:
                    # Atualiza apenas o banco selecionado
                    conn.execute(f"""
                        UPDATE saldos_bancos
                        SET "{banco_selecionado}" = ?
                        WHERE data = ?
                    """, (valor_digitado, data_str))
                conn.commit()

            valor_fmt = f"R$ {valor_digitado:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            # Salva mensagem de sucesso na sessão
            st.session_state["mensagem_sucesso"] = f"✅ Saldo de {valor_fmt} para **{banco_selecionado}** salvo com sucesso!"
            st.rerun()

        except Exception as e:
            st.error(f"❌ Erro ao salvar saldo: {e}")

    # --- Últimos registros ---
    st.markdown("---")
    st.markdown("### 📋 Últimos Saldos Registrados")

    try:
        with sqlite3.connect(caminho_banco) as conn:
            df_saldos = pd.read_sql("SELECT * FROM saldos_bancos ORDER BY data DESC LIMIT 15", conn)

        if not df_saldos.empty:
            df_saldos["data"] = pd.to_datetime(df_saldos["data"]).dt.strftime("%d/%m/%Y")
            for banco in bancos:
                if banco in df_saldos.columns:
                    df_saldos[banco] = df_saldos[banco].apply(
                        lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    )
            df_saldos = df_saldos.rename(columns={"data": "Data"})
            st.dataframe(df_saldos, use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ Nenhum saldo registrado ainda.")

    except Exception as e:
        st.error(f"Erro ao carregar os saldos: {e}")