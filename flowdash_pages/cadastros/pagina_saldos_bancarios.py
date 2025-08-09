import streamlit as st
import sqlite3
import pandas as pd
from datetime import date
from flowdash_pages.cadastros.cadastro_classes import BancoRepository

def _inserir_mov_bancaria(caminho_banco, data_, banco, valor):
    """
    Insere em movimentacoes_bancarias uma ENTRADA (valor > 0) com origem 'saldos_bancos'.
    Monta o INSERT conforme as colunas realmente existentes na tabela.
    """
    if valor is None or float(valor) <= 0:
        return

    with sqlite3.connect(caminho_banco) as conn:
        cols_info = conn.execute("PRAGMA table_info(movimentacoes_bancarias)").fetchall()
        if not cols_info:
            # Se não houver tabela, não há o que fazer silenciosamente
            return

        cols_exist = {c[1] for c in cols_info}

        payload = {
            "data": str(data_),
            "banco": banco,
            "tipo": "entrada",
            "valor": float(valor),
            "origem": "saldos_bancos",
            "observacao": "Registro manual de saldo bancário",
            # "referencia_id": None,  # use se existir
        }

        cols_use = [k for k in payload if k in cols_exist]
        vals_use = [payload[k] for k in cols_use]

        placeholders = ",".join(["?"] * len(cols_use))
        cols_sql = ",".join(f'"{c}"' for c in cols_use)

        conn.execute(f"INSERT INTO movimentacoes_bancarias ({cols_sql}) VALUES ({placeholders})", vals_use)
        conn.commit()

def pagina_saldos_bancarios(caminho_banco: str):
    st.subheader("🏦 Cadastro de Saldos Bancários por Banco (append-only)")

    # Mensagem persistente pós-rerun
    if "mensagem_sucesso" in st.session_state:
        st.success(st.session_state.pop("mensagem_sucesso"))

    # Data do lançamento
    data_sel = st.date_input("📅 Data do lançamento", value=date.today())
    data_str = str(data_sel)

    # Bancos cadastrados
    repo_banco = BancoRepository(caminho_banco)
    df_bancos = repo_banco.carregar_bancos()

    if df_bancos.empty:
        st.warning("⚠️ Nenhum banco cadastrado. Cadastre um banco primeiro.")
        return

    bancos = df_bancos["nome"].tolist()
    banco_selecionado = st.selectbox("🏦 Banco", bancos)
    valor_digitado = st.number_input(
        "💰 Valor do saldo (será adicionado como novo registro)",
        min_value=0.0, step=10.0, format="%.2f"
    )

    if st.button("💾 Cadastrar Saldo (nova linha)", use_container_width=True):
        try:
            # nova linha com 0.0 em todos os bancos e valor no selecionado
            nova_linha = {b: 0.0 for b in bancos}
            nova_linha[banco_selecionado] = float(valor_digitado)
            nova_linha["data"] = data_str

            with sqlite3.connect(caminho_banco) as conn:
                # insere (append-only)
                pd.DataFrame([nova_linha]).to_sql("saldos_bancos", conn, if_exists="append", index=False)

            # também lança em movimentacoes_bancarias como ENTRADA
            _inserir_mov_bancaria(caminho_banco, data_str, banco_selecionado, valor_digitado)

            valor_fmt = f"R$ {valor_digitado:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            st.session_state["mensagem_sucesso"] = (
                f"✅ Valor {valor_fmt} foi cadastrado em **{banco_selecionado}** "
                f"(data {pd.to_datetime(data_str).strftime('%d/%m/%Y')})."
            )
            st.rerun()

        except Exception as e:
            st.error(f"❌ Erro ao cadastrar saldo: {e}")

    # --- Últimos lançamentos (append-only) ---
    st.markdown("---")
    st.markdown("### 📋 Últimos Lançamentos (saldos_bancos)")

    try:
        with sqlite3.connect(caminho_banco) as conn:
            # ordena por id se existir; senão por data
            cols_info = conn.execute("PRAGMA table_info(saldos_bancos)").fetchall()
            cols_existentes = {c[1] for c in cols_info}
            order_sql = "ORDER BY id DESC" if "id" in cols_existentes else "ORDER BY data DESC"
            df_saldos = pd.read_sql(f"SELECT * FROM saldos_bancos {order_sql} LIMIT 30", conn)

        if not df_saldos.empty:
            if "data" in df_saldos.columns:
                df_saldos["data"] = pd.to_datetime(df_saldos["data"], errors="coerce").dt.strftime("%d/%m/%Y")

            for banco in bancos:
                if banco in df_saldos.columns:
                    df_saldos[banco] = df_saldos[banco].apply(
                        lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                        if pd.notnull(x) else ""
                    )

            if "data" in df_saldos.columns:
                df_saldos = df_saldos.rename(columns={"data": "Data"})

            st.dataframe(df_saldos, use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ Nenhum lançamento registrado ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar os lançamentos: {e}")

    # --- Resumo diário por banco (somatório do dia) ---
    st.markdown("---")
    st.markdown("### 📆 Resumo Diário por Banco (somatório do dia)")

    try:
        with sqlite3.connect(caminho_banco) as conn:
            df_raw = pd.read_sql("SELECT * FROM saldos_bancos", conn)

        if df_raw.empty or "data" not in df_raw.columns:
            st.info("Ainda não há lançamentos para resumir.")
            return

        df_raw["data"] = pd.to_datetime(df_raw["data"], errors="coerce")
        if df_raw["data"].isna().all():
            st.warning("Não foi possível interpretar datas em saldos_bancos.")
            return

        col_bancos_existentes = [b for b in bancos if b in df_raw.columns]
        for b in col_bancos_existentes:
            df_raw[b] = pd.to_numeric(df_raw[b], errors="coerce").fillna(0.0)

        df_resumo = df_raw.groupby(df_raw["data"].dt.date)[col_bancos_existentes].sum().reset_index()
        df_resumo = df_resumo.rename(columns={"data": "Data"})
        df_resumo["Data"] = pd.to_datetime(df_resumo["Data"]).dt.strftime("%d/%m/%Y")

        df_fmt = df_resumo.copy()
        for b in col_bancos_existentes:
            df_fmt[b] = df_fmt[b].apply(
                lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

        st.markdown("**Visão Resumida (largura por banco):**")
        st.dataframe(df_fmt, use_container_width=True, hide_index=True)

        st.markdown("**Visão Detalhada (uma linha por banco/dia):**")
        df_long = df_resumo.melt(id_vars=["Data"], value_vars=col_bancos_existentes,
                                 var_name="Banco", value_name="Total do Dia")
        df_long["Total do Dia"] = df_long["Total do Dia"].apply(
            lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        )
        st.dataframe(df_long.sort_values(["Data", "Banco"]), use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Erro ao calcular o resumo diário: {e}")