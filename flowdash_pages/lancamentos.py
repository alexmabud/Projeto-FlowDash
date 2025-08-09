import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, datetime, timedelta

from utils.utils import formatar_valor


# =========================
# Helpers gerais da página
# =========================

DIAS_COMPENSACAO = {
    "DINHEIRO": 0,
    "PIX": 0,
    "DÉBITO": 1,
    "CRÉDITO": 1,
    "LINK_PAGAMENTO": 1,
}

def proximo_dia_util_br(data_base: date, dias: int) -> date:
    """
    Retorna a data de liquidação em 'dias' úteis à frente.
    - Tenta usar feriados de Brasília (workalendar). Se não houver, considera apenas fins de semana.
    """
    try:
        from workalendar.america import BrazilDistritoFederal
        cal = BrazilDistritoFederal()
        d = data_base
        adicionados = 0
        while adicionados < dias:
            d += timedelta(days=1)
            if cal.is_working_day(d):
                adicionados += 1
        return d
    except Exception:
        # fallback: fins de semana apenas
        d = data_base
        adicionados = 0
        while adicionados < dias:
            d += timedelta(days=1)
            if d.weekday() < 5:  # 0=seg ... 6=dom
                adicionados += 1
        return d

def carregar_tabela(nome_tabela, caminho_banco):
    try:
        with sqlite3.connect(caminho_banco) as conn:
            df = pd.read_sql(f"SELECT * FROM {nome_tabela}", conn)
            if "Data" in df.columns:
                df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.strftime("%Y-%m-%d")
            return df
    except Exception:
        return pd.DataFrame()

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

def inserir_mov_liquidacao_venda(caminho_banco: str, data_: str, banco: str, valor_liquido: float,
                                 observacao: str, referencia_id: int | None):
    if not valor_liquido or valor_liquido <= 0:
        return
    with sqlite3.connect(caminho_banco) as conn:
        conn.execute("""
            INSERT INTO movimentacoes_bancarias (data, banco, tipo, valor, origem, observacao, referencia_id)
            VALUES (?, ?, 'entrada', ?, 'vendas', ?, ?)
        """, (data_, banco, float(valor_liquido), observacao, referencia_id))
        conn.commit()

def obter_banco_destino(caminho_banco: str, forma: str, maquineta: str, bandeira: str | None, parcelas: int | None) -> str | None:
    """
    Descobre o banco_destino lendo a tabela taxas_maquinas.
    Estratégia:
      1) Match exato (forma, maquineta, bandeira, parcelas)
      2) Se forma == LINK_PAGAMENTO, tenta como CRÉDITO também (fallback comum)
      3) Match por (forma, maquineta) ignorando bandeira/parcelas
      4) Qualquer registro da maquineta
    """
    formas_try = [forma]
    if forma == "LINK_PAGAMENTO":
        formas_try.append("CRÉDITO")

    with sqlite3.connect(caminho_banco) as conn:
        for f in formas_try:
            row = conn.execute("""
                SELECT banco_destino
                FROM taxas_maquinas
                WHERE forma_pagamento = ?
                  AND maquineta       = ?
                  AND bandeira        = ?
                  AND parcelas        = ?
                LIMIT 1
            """, (f, maquineta or "", bandeira or "", int(parcelas or 1))).fetchone()
            if row and row[0]:
                return row[0]

        for f in formas_try:
            row = conn.execute("""
                SELECT banco_destino
                FROM taxas_maquinas
                WHERE forma_pagamento = ?
                  AND maquineta       = ?
                  AND banco_destino IS NOT NULL
                  AND TRIM(banco_destino) <> ''
                LIMIT 1
            """, (f, maquineta or "")).fetchone()
            if row and row[0]:
                return row[0]

        row = conn.execute("""
            SELECT banco_destino
            FROM taxas_maquinas
            WHERE maquineta = ?
              AND banco_destino IS NOT NULL
              AND TRIM(banco_destino) <> ''
            LIMIT 1
        """, (maquineta or "",)).fetchone()
        if row and row[0]:
            return row[0]

    return None


# =========================
# Página principal
# =========================

def pagina_lancamentos(caminho_banco):
    # Mensagem persistente de sucesso
    if "msg_ok" in st.session_state:
        st.success(st.session_state["msg_ok"])
        del st.session_state["msg_ok"]

    # Data do lançamento
    data_lancamento = st.date_input("🗓️ Selecione a Data do Lançamento", value=date.today(), key="data_lancamento")
    st.markdown(
        f"## 🧾 Lançamentos do Dia — <span style='color:#00FFAA'><b>{data_lancamento}</b></span>",
        unsafe_allow_html=True
    )
    data_str = str(data_lancamento)

    # === Resumo do dia ===
    st.markdown("### 📊 Resumo do Dia")
    df_entrada = carregar_tabela("entrada", caminho_banco)
    df_saida = carregar_tabela("saida", caminho_banco)
    df_mercadorias = carregar_tabela("mercadorias", caminho_banco)

    total_entrada = df_entrada[df_entrada["Data"] == data_str]["Valor"].sum() if "Valor" in df_entrada.columns else 0.0
    total_saida = df_saida[df_saida["Data"] == data_str]["Valor"].sum() if "Valor" in df_saida.columns else 0.0
    total_mercadorias = df_mercadorias[df_mercadorias["Data"] == data_str]["Valor_Mercadoria"].sum() if "Valor_Mercadoria" in df_mercadorias.columns else 0.0

    bloco_resumo_dia([
        ("Entradas", formatar_valor(total_entrada)),
        ("Saídas", formatar_valor(total_saida)),
        ("Mercadorias", formatar_valor(total_mercadorias))
    ])

    # === Ações ===
    st.markdown("### ➕ Ações do Dia")
    col1, col2 = st.columns(2)

    # -----------------------------
    # Coluna 1 — Nova Venda / Caixa 2
    # -----------------------------
    with col1:
        if st.button("🟢 Nova Venda", use_container_width=True):
            st.session_state.form_venda = not st.session_state.get("form_venda", False)

        if st.session_state.get("form_venda", False):
            st.markdown("#### 📋 Nova Venda")

            valor = st.number_input("Valor da Venda", min_value=0.0, step=0.01, key="valor_venda")
            forma_pagamento = st.selectbox(
                "Forma de Pagamento",
                ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"],
                key="forma_pagamento_venda"
            )

            parcelas = 1
            bandeira = ""
            maquineta = ""
            banco_pix_direto = None
            taxa_pix_direto = 0.0

            # Carrega maquinetas existes na taxa
            try:
                with sqlite3.connect(caminho_banco) as conn:
                    maquinetas_all = pd.read_sql("SELECT DISTINCT maquineta FROM taxas_maquinas ORDER BY maquineta", conn)["maquineta"].tolist()
            except Exception:
                maquinetas_all = []

            if forma_pagamento == "PIX":
                modo_pix = st.radio(
                    "Como será o PIX?",
                    ["Via maquineta", "Direto para banco"],
                    horizontal=True,
                    key="modo_pix"
                )
                if modo_pix == "Via maquineta":
                    # maquineta obrigatória para pix via maquineta
                    try:
                        with sqlite3.connect(caminho_banco) as conn:
                            maq_pix = pd.read_sql("""
                                SELECT DISTINCT maquineta
                                FROM taxas_maquinas
                                WHERE forma_pagamento = 'PIX'
                                ORDER BY maquineta
                            """, conn)["maquineta"].tolist()
                    except Exception:
                        maq_pix = []
                    maquineta = st.selectbox("PSP/Maquineta do PIX", maq_pix, key="maquineta_pix")
                    bandeira = ""
                    parcelas = 1
                else:
                    # PIX direto para banco
                    maquineta = ""
                    bandeira = ""
                    parcelas = 1
                    try:
                        with sqlite3.connect(caminho_banco) as conn:
                            df_bancos = pd.read_sql("SELECT nome FROM bancos_cadastrados ORDER BY nome", conn)
                        bancos_lista = df_bancos["nome"].tolist()
                    except Exception:
                        bancos_lista = []
                    banco_pix_direto = st.selectbox("Banco que receberá o PIX", bancos_lista, key="banco_pix_direto")
                    taxa_pix_direto = st.number_input("Taxa do PIX direto (%)", min_value=0.0, step=0.01, format="%.2f", value=0.0, key="taxa_pix_direto")

            elif forma_pagamento in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"]:
                maquineta = st.selectbox("Maquineta", maquinetas_all, key="maquineta_cartao")
                # Bandeira
                try:
                    with sqlite3.connect(caminho_banco) as conn:
                        bandeiras = pd.read_sql("""
                            SELECT DISTINCT bandeira FROM taxas_maquinas
                            WHERE forma_pagamento = ? AND maquineta = ?
                            ORDER BY bandeira
                        """, conn, params=(forma_pagamento if forma_pagamento != "LINK_PAGAMENTO" else "CRÉDITO", maquineta))["bandeira"].tolist()
                except Exception:
                    bandeiras = []
                bandeira = st.selectbox("Bandeira", bandeiras, key="bandeira_cartao") if bandeiras else ""

                # Parcelas
                if forma_pagamento in ["CRÉDITO", "LINK_PAGAMENTO"]:
                    try:
                        with sqlite3.connect(caminho_banco) as conn:
                            parcelas_disp = pd.read_sql("""
                                SELECT DISTINCT parcelas FROM taxas_maquinas
                                WHERE forma_pagamento = ? AND maquineta = ? AND bandeira = ?
                                ORDER BY parcelas
                            """, conn, params=(forma_pagamento if forma_pagamento != "LINK_PAGAMENTO" else "CRÉDITO", maquineta, bandeira))["parcelas"].tolist()
                    except Exception:
                        parcelas_disp = []
                    parcelas = st.selectbox("Parcelas", parcelas_disp if parcelas_disp else [1], key="parcelas_cartao")
                else:
                    parcelas = 1

            elif forma_pagamento == "DINHEIRO":
                modo_din = st.radio(
                    "Como registrar o dinheiro?",
                    ["Via maquineta", "Direto para banco"],
                    horizontal=True,
                    key="modo_dinheiro"
                )
                if modo_din == "Via maquineta":
                    maquineta = st.selectbox("Maquineta (para definir banco de destino)", maquinetas_all, key="maquineta_din")
                else:
                    maquineta = ""
                    try:
                        with sqlite3.connect(caminho_banco) as conn:
                            df_bancos = pd.read_sql("SELECT nome FROM bancos_cadastrados ORDER BY nome", conn)
                        bancos_lista = df_bancos["nome"].tolist()
                    except Exception:
                        bancos_lista = []
                    banco_pix_direto = st.selectbox("Banco que receberá (depósito do caixa)", bancos_lista, key="banco_dinheiro_direto")

            # Resumo antes de salvar
            confirmar = st.checkbox("Confirmo os dados para salvar a venda", key="confirmar_venda")

            if confirmar:
                resumo = [
                    f"- Valor: R$ {valor:,.2f}",
                    f"- Forma de pagamento: {forma_pagamento}",
                ]
                if maquineta:
                    resumo.append(f"- Maquineta: {maquineta}")
                if bandeira:
                    resumo.append(f"- Bandeira: {bandeira}")
                if forma_pagamento in ["CRÉDITO", "LINK_PAGAMENTO"]:
                    resumo.append(f"- Parcelas: {parcelas}")
                if forma_pagamento == "PIX" and st.session_state.get("modo_pix") == "Direto para banco" and banco_pix_direto:
                    resumo.append(f"- Banco PIX direto: {banco_pix_direto} (taxa {taxa_pix_direto:.2f}%)")
                if forma_pagamento == "DINHEIRO" and st.session_state.get("modo_dinheiro") == "Direto para banco" and banco_pix_direto:
                    resumo.append(f"- Banco destino (dinheiro): {banco_pix_direto}")

                st.info("**Resumo da Venda:**\n\n" + "\n".join(resumo))

            # ===== Salvar Venda =====
            if st.button("💾 Salvar Venda", use_container_width=True):
                # validações básicas
                if valor <= 0:
                    st.warning("⚠️ Valor inválido.")
                    st.stop()
                if not confirmar:
                    st.warning("⚠️ Confirme os dados antes de salvar.")
                    st.stop()
                if forma_pagamento in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"] and (not maquineta or not bandeira):
                    st.warning("⚠️ Selecione maquineta e bandeira.")
                    st.stop()
                if forma_pagamento == "PIX" and st.session_state.get("modo_pix") == "Via maquineta" and not maquineta:
                    st.warning("⚠️ Selecione a maquineta do PIX.")
                    st.stop()
                if forma_pagamento == "DINHEIRO" and st.session_state.get("modo_dinheiro") == "Via maquineta" and not maquineta:
                    st.warning("⚠️ Selecione a maquineta para definir o banco.")
                    st.stop()

                try:
                    # 1) calcular taxa e banco_destino
                    taxa = 0.0
                    banco_destino = None

                    if forma_pagamento in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"]:
                        # taxa e banco pela taxa cadastrada
                        with sqlite3.connect(caminho_banco) as conn:
                            row = conn.execute("""
                                SELECT taxa_percentual, banco_destino
                                FROM taxas_maquinas
                                WHERE forma_pagamento = ?
                                  AND maquineta       = ?
                                  AND bandeira        = ?
                                  AND parcelas        = ?
                                LIMIT 1
                            """, (
                                forma_pagamento if forma_pagamento != "LINK_PAGAMENTO" else "CRÉDITO",
                                maquineta, bandeira, int(parcelas or 1)
                            )).fetchone()
                        if row:
                            taxa = float(row[0] or 0.0)
                            banco_destino = row[1] if row[1] else None
                        # fallback
                        if not banco_destino:
                            banco_destino = obter_banco_destino(caminho_banco, forma_pagamento, maquineta, bandeira, parcelas)

                    elif forma_pagamento == "PIX":
                        if st.session_state.get("modo_pix") == "Via maquineta":
                            with sqlite3.connect(caminho_banco) as conn:
                                row = conn.execute("""
                                    SELECT taxa_percentual, banco_destino
                                    FROM taxas_maquinas
                                    WHERE forma_pagamento = 'PIX'
                                      AND maquineta       = ?
                                      AND bandeira        = ''
                                      AND parcelas        = 1
                                    LIMIT 1
                                """, (maquineta,)).fetchone()
                            taxa = float(row[0] or 0.0) if row else 0.0
                            banco_destino = (row[1] if row and row[1] else None)
                            if not banco_destino:
                                banco_destino = obter_banco_destino(caminho_banco, "PIX", maquineta, "", 1)
                        else:
                            # PIX direto para banco
                            banco_destino = banco_pix_direto
                            taxa = float(taxa_pix_direto or 0.0)
                            if not banco_destino:
                                st.warning("⚠️ Selecione o banco que receberá o PIX direto.")
                                st.stop()

                    elif forma_pagamento == "DINHEIRO":
                        taxa = 0.0
                        if st.session_state.get("modo_dinheiro") == "Via maquineta":
                            banco_destino = obter_banco_destino(caminho_banco, "DINHEIRO", maquineta, "", 1)
                        else:
                            banco_destino = banco_pix_direto  # aqui reaproveitamos a var como "banco escolhido"
                        if not banco_destino:
                            st.warning("⚠️ Não foi possível identificar o banco de destino para o DINHEIRO.")
                            st.stop()

                    # 2) calcula valor líquido
                    valor_liquido = float(valor) * (1 - float(taxa) / 100.0)

                    # 3) grava na ENTRADA (com valor_liquido)
                    with sqlite3.connect(caminho_banco) as conn:
                        usuario = st.session_state.usuario_logado["nome"] if "usuario_logado" in st.session_state and st.session_state.usuario_logado else "Sistema"
                        cur = conn.execute("""
                            INSERT INTO entrada (Data, Valor, Forma_de_Pagamento, Parcelas, Bandeira, Usuario, maquineta, valor_liquido, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            str(data_lancamento),
                            float(valor),
                            forma_pagamento,
                            int(parcelas or 1),
                            bandeira,
                            usuario,
                            maquineta,
                            round(valor_liquido, 2),
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        ))
                        venda_id = cur.lastrowid
                        conn.commit()

                    # 4) agenda/insere liquidação no banco
                    dias = DIAS_COMPENSACAO.get(forma_pagamento, 0)
                    data_base = pd.to_datetime(data_lancamento).date()
                    data_liq = proximo_dia_util_br(data_base, dias) if dias > 0 else data_base

                    obs = f"Liquidação {forma_pagamento} {maquineta or ''}{('/' + bandeira) if bandeira else ''} {int(parcelas or 1)}x".strip()

                    if not banco_destino:
                        st.warning("⚠️ Não foi possível identificar o banco de destino. A movimentação NÃO foi lançada.")
                    else:
                        inserir_mov_liquidacao_venda(
                            caminho_banco=caminho_banco,
                            data_=str(data_liq),
                            banco=banco_destino,
                            valor_liquido=round(valor_liquido if forma_pagamento in ["PIX", "DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"] else float(valor), 2),
                            observacao=obs,
                            referencia_id=venda_id
                        )

                    # Mensagem persistente
                    st.session_state["msg_ok"] = (
                        f"✅ Venda registrada! "
                        f"{'Liquidação' if forma_pagamento!='DINHEIRO' else 'Registro'} de "
                        f"**{formatar_valor(valor_liquido if forma_pagamento in ['PIX','DÉBITO','CRÉDITO','LINK_PAGAMENTO'] else valor)}** "
                        f"em **{banco_destino or '—'}** na data **{data_liq.strftime('%d/%m/%Y')}**."
                    )

                    # Debug rápido das últimas liquidações
                    try:
                        with sqlite3.connect(caminho_banco) as conn:
                            df_dbg = pd.read_sql("""
                                SELECT id, data, banco, tipo, valor, origem, observacao, referencia_id
                                FROM movimentacoes_bancarias
                                WHERE origem = 'vendas'
                                ORDER BY id DESC
                                LIMIT 10
                            """, conn)
                        if not df_dbg.empty:
                            st.caption("🔎 Últimos lançamentos de liquidação (origem = vendas):")
                            st.dataframe(df_dbg, use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.warning(f"Não consegui listar as movimentações: {e}")

                    st.session_state.form_venda = False
                    st.rerun()

                except Exception as e:
                    st.error(f"Erro ao salvar venda: {e}")

        # Caixa 2 (placeholder)
        with st.container():
            if st.button("🔄 Caixa 2", use_container_width=True):
                st.session_state.form_caixa2 = not st.session_state.get("form_caixa2", False)
            if st.session_state.get("form_caixa2", False):
                st.markdown("#### 💸 Transferência para Caixa 2")
                st.number_input("Valor a Transferir", min_value=0.0, step=0.01, key="valor_caixa2")
                st.button("💾 Confirmar Transferência", use_container_width=True)

    # -----------------------------
    # Coluna 2 — Saída / Depósito
    # -----------------------------
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

    # Linha separada para Mercadorias (placeholder)
    st.markdown("---")
    with st.container():
        if st.button("📦 Mercadorias", use_container_width=True):
            st.session_state.form_mercadoria = not st.session_state.get("form_mercadoria", False)
        if st.session_state.get("form_mercadoria", False):
            st.markdown("#### 📦 Registro de Mercadorias")
            st.text_input("Fornecedor", key="fornecedor")
            st.number_input("Valor da Mercadoria", min_value=0.0, step=0.01, key="valor_mercadoria")
            st.button("💾 Salvar Mercadoria", use_container_width=True)