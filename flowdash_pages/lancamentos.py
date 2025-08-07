import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, datetime

from utils.utils import formatar_valor


def pagina_lancamentos(caminho_banco):
    # Campo para seleção de data do lançamento (mover para o topo)
    data_lancamento = st.date_input("🗓️ Selecione a Data do Lançamento", value=date.today(), key="data_lancamento")
    st.markdown(f"## 🧾 Lançamentos do Dia — <span style='color:#00FFAA'><b>{data_lancamento}</b></span>", unsafe_allow_html=True)

    data_str = str(data_lancamento)

    # === RESUMO DO DIA ==================================================================================
    st.markdown("### 📊 Resumo do Dia")
    df_entrada = carregar_tabela("entrada", caminho_banco)
    df_saida = carregar_tabela("saida", caminho_banco)
    df_mercadorias = carregar_tabela("mercadorias", caminho_banco)

    total_entrada = df_entrada[df_entrada["Data"] == data_str]["Valor"].sum()
    total_saida = df_saida[df_saida["Data"] == data_str]["Valor"].sum()
    total_mercadorias = df_mercadorias[df_mercadorias["Data"] == data_str]["Valor_Mercadoria"].sum()

    bloco_resumo_dia([
        ("Entradas", formatar_valor(total_entrada)),
        ("Saídas", formatar_valor(total_saida)),
        ("Mercadorias", formatar_valor(total_mercadorias))
    ])

    # === BOTÕES E FORMULÁRIOS ==========================================================================
    st.markdown("### ➕ Ações do Dia")
    col1, col2 = st.columns(2)

    # Coluna 1 - Nova Venda e Caixa 2
    with col1:
        with st.container():
            if st.button("🟢 Nova Venda", use_container_width=True):
                st.session_state.form_venda = not st.session_state.get("form_venda", False)
            if st.session_state.get("form_venda", False):
                st.markdown("#### 📋 Nova Venda")
                valor = st.number_input("Valor da Venda", min_value=0.0, step=0.01, key="valor_venda")
                forma_pagamento = st.selectbox("Forma de Pagamento", ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO"], key="forma_pagamento_venda")

                maquineta, bandeira, parcelas = "", "", 1
                if forma_pagamento in ["PIX", "DÉBITO", "CRÉDITO"]:
                    with sqlite3.connect(caminho_banco) as conn:
                        maquinetas = pd.read_sql("SELECT DISTINCT maquineta FROM taxas_maquinas ORDER BY maquineta", conn)["maquineta"].tolist()
                    maquineta = st.selectbox("Maquineta", maquinetas, key="maquineta")

                    if forma_pagamento in ["DÉBITO", "CRÉDITO"]:
                        with sqlite3.connect(caminho_banco) as conn:
                            bandeiras = pd.read_sql("""
                                SELECT DISTINCT bandeira FROM taxas_maquinas
                                WHERE forma_pagamento = ? AND maquineta = ?
                                ORDER BY bandeira
                            """, conn, params=(forma_pagamento, maquineta))["bandeira"].tolist()
                        bandeira = st.selectbox("Bandeira", bandeiras, key="bandeira")

                        if forma_pagamento == "CRÉDITO":
                            with sqlite3.connect(caminho_banco) as conn:
                                parcelas_disp = pd.read_sql("""
                                    SELECT DISTINCT parcelas FROM taxas_maquinas
                                    WHERE forma_pagamento = ? AND bandeira = ? AND maquineta = ?
                                    ORDER BY parcelas
                                """, conn, params=(forma_pagamento, bandeira, maquineta))["parcelas"].tolist()
                            parcelas = st.selectbox("Parcelas", parcelas_disp, key="parcelas")

                confirmar = st.checkbox("Confirmo os dados para salvar a venda", key="confirmar_venda")

                # Exibe resumo antes de salvar
                if confirmar:
                    st.info(
                        f"**Resumo da Venda:**\n\n"
                        f"- Valor: R$ {valor:,.2f}\n"
                        f"- Forma de pagamento: {forma_pagamento}\n"
                        f"{f'- Maquineta: {maquineta}' if maquineta else ''}\n"
                        f"{f'- Bandeira: {bandeira}' if bandeira else ''}\n"
                        f"{f'- Parcelas: {parcelas}' if forma_pagamento == 'CRÉDITO' else ''}"
                    )

                if st.button("💾 Salvar Venda", use_container_width=True):
                    if valor <= 0:
                        st.warning("⚠️ Valor inválido.")
                    elif forma_pagamento in ["PIX", "DÉBITO", "CRÉDITO"] and not maquineta:
                        st.warning("⚠️ Selecione uma maquineta.")
                    elif forma_pagamento in ["DÉBITO", "CRÉDITO"] and not bandeira:
                        st.warning("⚠️ Selecione uma bandeira.")
                    elif forma_pagamento == "CRÉDITO" and not parcelas:
                        st.warning("⚠️ Selecione o número de parcelas.")
                    elif not confirmar:
                        st.warning("⚠️ Confirme os dados antes de salvar.")
                    else:
                        try:
                            valor_liquido = valor
                            if forma_pagamento in ["PIX", "DÉBITO", "CRÉDITO"]:
                                with sqlite3.connect(caminho_banco) as conn:
                                    cursor = conn.execute(
                                        """
                                        SELECT taxa_percentual FROM taxas_maquinas
                                        WHERE forma_pagamento = ? AND maquineta = ? AND bandeira = ? AND parcelas = ?
                                        """,
                                        (forma_pagamento, maquineta, bandeira, parcelas)
                                    )
                                    row = cursor.fetchone()
                                    taxa = row[0] if row else 0.0
                                    valor_liquido = valor * (1 - taxa / 100)

                            with sqlite3.connect(caminho_banco) as conn:
                                usuario = st.session_state.usuario_logado["nome"]
                                conn.execute(
                                    """
                                    INSERT INTO entrada (Data, Valor, Forma_de_Pagamento, Parcelas, Bandeira, Usuario, maquineta, valor_liquido, created_at)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        str(data_lancamento),
                                        float(valor),
                                        forma_pagamento,
                                        parcelas,
                                        bandeira,
                                        usuario,
                                        maquineta,
                                        round(valor_liquido, 2),
                                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    )
                                )

                                # Se a venda for em dinheiro, atualiza saldos_caixas
                                if forma_pagamento == "DINHEIRO":
                                    df_saldos = pd.read_sql("SELECT * FROM saldos_caixas", conn)
                                    df_saldos["data"] = pd.to_datetime(df_saldos["data"], errors="coerce").dt.date
                                    data_dt = pd.to_datetime(data_lancamento).date()

                                    if data_dt in df_saldos["data"].values:
                                        conn.execute("""
                                            UPDATE saldos_caixas
                                            SET caixa_vendas = COALESCE(caixa_vendas, 0) + ?
                                            WHERE DATE(data) = DATE(?)
                                        """, (valor, data_lancamento))
                                    else:
                                        conn.execute("""
                                            INSERT INTO saldos_caixas (data, caixa_vendas)
                                            VALUES (?, ?)
                                        """, (data_lancamento, valor))

                                conn.commit()

                            st.success("✅ Venda registrada com sucesso!")
                            st.session_state.form_venda = False
                            st.rerun()

                        except Exception as e:
                            st.error(f"Erro ao salvar venda: {e}")


        with st.container():
            if st.button("🔄 Caixa 2", use_container_width=True):
                st.session_state.form_caixa2 = not st.session_state.get("form_caixa2", False)
            if st.session_state.get("form_caixa2", False):
                st.markdown("#### 💸 Transferência para Caixa 2")
                st.number_input("Valor a Transferir", min_value=0.0, step=0.01, key="valor_caixa2")
                st.button("💾 Confirmar Transferência", use_container_width=True)

    # Coluna 2 - Saída e Depósito
    with col2:
        with st.container():
            if st.button("🔴 Saída", use_container_width=True):
                st.session_state.form_saida = not st.session_state.get("form_saida", False)
            if st.session_state.get("form_saida", False):
                st.markdown("#### 📋 Registrar Saída")
                st.number_input("Valor da Saída", min_value=0.0, step=0.01, key="valor_saida")
                st.selectbox("Categoria", ["Contas Fixas", "Contas"], key="categoria_saida")
                st.button("💾 Salvar Saída", use_container_width=True)

        with st.container():
            if st.button("🏦 Depósito Bancário", use_container_width=True):
                st.session_state.form_deposito = not st.session_state.get("form_deposito", False)
            if st.session_state.get("form_deposito", False):
                st.markdown("#### 🏦 Registrar Depósito Bancário")
                st.number_input("Valor Depositado", min_value=0.0, step=0.01, key="valor_deposito")
                st.selectbox("Banco Destino", ["Banco 1", "Banco 2", "Banco 3", "Banco 4"], key="banco_destino")
                st.button("💾 Salvar Depósito", use_container_width=True)

    # Linha separada para Mercadorias
    st.markdown("---")
    with st.container():
        if st.button("📦 Mercadorias", use_container_width=True):
            st.session_state.form_mercadoria = not st.session_state.get("form_mercadoria", False)
        if st.session_state.get("form_mercadoria", False):
            st.markdown("#### 📦 Registro de Mercadorias")
            st.text_input("Fornecedor", key="fornecedor")
            st.number_input("Valor da Mercadoria", min_value=0.0, step=0.01, key="valor_mercadoria")
            st.button("💾 Salvar Mercadoria", use_container_width=True)


# === COMPONENTES AUXILIARES ======================================================================================
def bloco_resumo_dia(itens):
    st.markdown(f"""
    <div style='border: 1px solid #444; border-radius: 10px; padding: 20px; background-color: #1c1c1c; margin-bottom: 20px;'>
        <h4 style='color: white;'>📆 Resumo Financeiro de Hoje</h4>
        <table style='width: 100%; margin-top: 15px;'>
            <tr>
                {''.join([
                    f"<td style='text-align: center; width: 33%;'>"
                    f"<div style='color: #ccc; font-weight: bold;'>{label}</div>"
                    f"<div style='font-size: 1.6rem; color: #00FFAA;'>{valor}</div>"
                    f"</td>"
                    for label, valor in itens
                ])}
            </tr>
        </table>
    </div>
    """, unsafe_allow_html=True)


def carregar_tabela(nome_tabela, caminho_banco):
    try:
        with sqlite3.connect(caminho_banco) as conn:
            df = pd.read_sql(f"SELECT * FROM {nome_tabela}", conn)
            df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.strftime("%Y-%m-%d")
            return df
    except:
        return pd.DataFrame()
