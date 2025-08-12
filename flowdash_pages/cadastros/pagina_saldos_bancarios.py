import streamlit as st
import sqlite3
import pandas as pd
from datetime import date
from repository.movimentacoes_repository import MovimentacoesRepository
from flowdash_pages.cadastros.cadastro_classes import BancoRepository


def _inserir_mov_bancaria(caminho_banco, data_, banco, valor, referencia_id=None):
    """
    Registra ENTRADA em movimentacoes_bancarias (valor > 0) com origem 'saldos_bancos',
    usando MovimentacoesRepository (idempotente).
    """
    try:
        if valor is None or float(valor) <= 0:
            return
        mov_repo = MovimentacoesRepository(caminho_banco)
        mov_repo.registrar_entrada(
            data=str(data_),
            banco=str(banco or ""),
            valor=float(valor),
            origem="saldos_bancos",
            observacao="Registro manual de saldo bancário",
            referencia_tabela="saldos_bancos",
            referencia_id=referencia_id
        )
    except Exception as e:
        st.warning(f"Não foi possível registrar movimentação para {banco}: {e}")

def _garantir_colunas_bancos(conn: sqlite3.Connection, bancos: list[str]) -> None:
    """
    Garante que todas as colunas referentes aos bancos existam na tabela saldos_bancos.
    Se faltar alguma, cria com DEFAULT 0.0.
    """
    cols_info = conn.execute("PRAGMA table_info(saldos_bancos)").fetchall()
    existentes = {c[1] for c in cols_info}
    faltantes = [b for b in bancos if b not in existentes]
    for b in faltantes:
        conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{b}" REAL DEFAULT 0.0')

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
        "💰 Valor do saldo (será adicionado como nova linha)",
        min_value=0.0, step=10.0, format="%.2f"
    )

    if st.button("💾 Cadastrar Saldo (nova linha)", use_container_width=True):
        try:
            with sqlite3.connect(caminho_banco) as conn:
                # Garante as colunas para todos os bancos
                _garantir_colunas_bancos(conn, bancos)

                # Monta INSERT dinâmico: data + uma coluna por banco
                colunas = ["data"] + bancos
                valores = [data_str] + [
                    float(valor_digitado) if b == banco_selecionado else 0.0
                    for b in bancos
                ]
                placeholders = ",".join(["?"] * len(colunas))
                colunas_sql = ",".join([f'"{c}"' for c in colunas])

                cur = conn.cursor()
                cur.execute(
                    f'INSERT INTO saldos_bancos ({colunas_sql}) VALUES ({placeholders})',
                    valores
                )
                saldo_id = int(cur.lastrowid)  # funciona mesmo sem coluna 'id'
                conn.commit()

            # também lança em movimentacoes_bancarias como ENTRADA, com referência amarrada
            _inserir_mov_bancaria(
                caminho_banco=caminho_banco,
                data_=data_str,
                banco=banco_selecionado,
                valor=valor_digitado,
                referencia_id=saldo_id
            )

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