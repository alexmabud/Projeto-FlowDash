"""
Módulo Mercadorias (Lançamentos)
================================

Este módulo define a página e a lógica para registrar e visualizar 
**mercadorias** no sistema FlowDash. Ele controla compras de estoque, 
valores de frete e prazos de recebimento, permitindo acompanhamento 
integrado com o fluxo financeiro.

Funcionalidades principais
--------------------------
- Registro de novas mercadorias com informações detalhadas:
  - Data de compra
  - Coleção / fornecedor
  - Valor das mercadorias
  - Frete
  - Forma de pagamento
  - Previsão de faturamento e recebimento
  - Número do pedido e da nota fiscal
- Controle de previsões (faturamento e recebimento) para 
  alinhamento com fluxo de caixa.
- Exibição em tabela no mesmo formato das páginas de Entradas 
  e Saídas (filtros por ano/mês, totais acumulados, botões de seleção).
- Integração futura com o módulo de Estoque.

Detalhes técnicos
-----------------
- Implementado em Streamlit.
- Usa o banco de dados SQLite via repositórios para salvar e consultar mercadorias.
- Formatação monetária e de datas padronizada através de funções auxiliares.
- Pensado para relatórios gerenciais de estoque e DRE.

Dependências
------------
- streamlit
- pandas
- datetime
- shared.db.get_conn
- utils.utils (funções de formatação)
- repositórios de apoio (categorias, bancos, movimentações, quando aplicável)

"""

import streamlit as st
from datetime import date
from shared.db import get_conn
from utils.utils import formatar_valor


# =========================
# Helpers
# =========================
def _to_float_or_none(x):
    if x is None:
        return None
    try:
        s = str(x).strip().replace(",", ".")
        return float(s) if s != "" else None
    except Exception:
        return None

def _ensure_extra_cols(conn):
    """
    Garante colunas extras usadas no recebimento:
      - Valor_Recebido (REAL)
      - Frete_Cobrado (REAL)
      - Recebimento_Obs (TEXT)
    Pode rodar quantas vezes quiser (idempotente).
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(mercadorias);")
    cols = {r[1] for r in cur.fetchall()}
    to_add = []
    if "Valor_Recebido" not in cols:
        to_add.append('ALTER TABLE mercadorias ADD COLUMN Valor_Recebido REAL;')
    if "Frete_Cobrado" not in cols:
        to_add.append('ALTER TABLE mercadorias ADD COLUMN Frete_Cobrado REAL;')
    if "Recebimento_Obs" not in cols:
        to_add.append('ALTER TABLE mercadorias ADD COLUMN Recebimento_Obs TEXT;')
    for sql in to_add:
        cur.execute(sql)
    if to_add:
        conn.commit()

# =========================================================
# 🧾 COMPRA DE MERCADORIAS (apenas cadastro + previsões)
# =========================================================
def render_merc_compra(caminho_banco: str, data_lanc: date):
    # Toggle de visibilidade — use chave DIFERENTE da do st.form
    if st.button("🧾 Compra de Mercadorias", use_container_width=True, key="btn_merc_compra_toggle"):
        st.session_state["show_merc_compra"] = not st.session_state.get("show_merc_compra", False)
        # Ao abrir, limpe o checkbox ANTES dele ser criado neste run
        if st.session_state["show_merc_compra"]:
            if "merc_compra_confirma_out" in st.session_state:
                del st.session_state["merc_compra_confirma_out"]
        st.rerun()

    if not st.session_state.get("show_merc_compra", False):
        return

    st.markdown("#### 🧾 Compra de Mercadorias")

    with st.form("form_merc_compra"):
        # Linha 1 — Data (do topo), Coleção, Fornecedor
        c1, c2, c3 = st.columns([1, 1, 1.4])
        with c1:
            st.text_input("Data (YYYY-MM-DD)", value=str(data_lanc), disabled=True, key="merc_compra_data_display")
            data_txt = str(data_lanc)
        with c2:
            colecao = st.text_input("Coleção", key="merc_compra_colecao")
        with c3:
            fornecedor = st.text_input("Fornecedor", key="merc_compra_fornecedor")

        # Linha 2 — Valores e pagamento
        c4, c5, c6, c7 = st.columns([1, 1, 1, 1])
        with c4:
            valor_mercadoria = st.number_input("Valor da Mercadoria (R$)", min_value=0.0, step=0.01, key="merc_compra_valor")
        with c5:
            frete = st.number_input("Frete (R$)", min_value=0.0, step=0.01, key="merc_compra_frete")
        with c6:
            forma_opts = ["PIX", "BOLETO", "CRÉDITO", "DÉBITO", "DINHEIRO", "OUTRO"]
            forma_sel = st.selectbox("Forma de Pagamento", forma_opts, key="merc_compra_forma_sel")
        with c7:
            parcelas = st.number_input("Parcelas", min_value=1, max_value=360, step=1, value=1, key="merc_compra_parcelas")

        forma_pagamento = (
            st.text_input("Informe a forma de pagamento (OUTRO)", key="merc_compra_forma_outro").strip().upper()
            if forma_sel == "OUTRO" else forma_sel
        )
        if forma_pagamento == "CRÉDITO":
            st.caption(f"Parcelas: **{int(parcelas)}×**")

        # Linha 3 — Previsões (somente calendário)
        st.markdown("###### Previsões")
        p1, p2 = st.columns(2)
        with p1:
            prev_fat_dt = st.date_input("Previsão de Faturamento", value=data_lanc, key="merc_compra_prev_fat_dt")
        with p2:
            prev_rec_dt = st.date_input("Previsão de Recebimento", value=data_lanc, key="merc_compra_prev_rec_dt")

        # Linha 4 — N° Pedido / N° NF
        n1, n2 = st.columns(2)
        with n1:
            numero_pedido_str = st.text_input("Número do Pedido", key="merc_compra_num_pedido")
        with n2:
            numero_nf_str = st.text_input("Número da Nota Fiscal", key="merc_compra_num_nf")

        submitted = st.form_submit_button(
            "💾 Salvar Compra",
            use_container_width=True,
            disabled=not st.session_state.get("merc_compra_confirma_out", False)
        )

    # ✅ Checkbox posicionado ABAIXO do formulário (visual)
    st.checkbox("Confirmo os dados", key="merc_compra_confirma_out")

    if not submitted:
        return

    # Validações mínimas + server-side do checkbox
    if not st.session_state.get("merc_compra_confirma_out", False):
        st.warning("⚠️ Marque 'Confirmo os dados' para salvar.")
        return

    colecao = (colecao or "").strip()
    fornecedor = (fornecedor or "").strip()

    if not fornecedor or valor_mercadoria <= 0:
        st.warning("⚠️ Informe fornecedor e um valor de mercadoria maior que zero.")
        return

    # Conversões
    frete_f = float(frete) if frete is not None else None
    try:
        parcelas_int = int(parcelas)
    except Exception:
        parcelas_int = 1
    if forma_pagamento == "CRÉDITO" and parcelas_int < 1:
        st.warning("⚠️ Em CRÉDITO, defina Parcelas ≥ 1.")
        return
    if parcelas_int < 1:
        parcelas_int = 1
    numero_pedido = _to_float_or_none(numero_pedido_str)
    numero_nf = _to_float_or_none(numero_nf_str)
    previsao_faturamento = str(prev_fat_dt) if prev_fat_dt else None
    previsao_recebimento = str(prev_rec_dt) if prev_rec_dt else None

    try:
        with get_conn(caminho_banco) as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO mercadorias (
                    Data, Colecao, Fornecedor, Valor_Mercadoria, Frete,
                    Forma_Pagamento, Parcelas,
                    Previsao_Faturamento, Faturamento,
                    Previsao_Recebimento, Recebimento,
                    Numero_Pedido, Numero_NF
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data_txt, colecao, fornecedor, float(valor_mercadoria), frete_f,
                forma_pagamento, int(parcelas_int),
                previsao_faturamento, None,
                previsao_recebimento, None,
                numero_pedido, numero_nf
            ))
            conn.commit()

        st.session_state["msg_ok"] = "✅ Compra registrada com sucesso!"
        st.session_state["show_merc_compra"] = False
        st.rerun()

    except Exception as e:
        st.error(f"❌ Erro ao salvar compra: {e}")

# =========================================================
# 📥 RECEBIMENTO DE MERCADORIAS (efetivos + divergências)
# =========================================================
def render_merc_recebimento(caminho_banco: str, data_lanc: date):
    # Toggle de visibilidade — chave separada
    if st.button("📥 Recebimento de Mercadorias", use_container_width=True, key="btn_merc_receb_toggle"):
        st.session_state["show_merc_receb"] = not st.session_state.get("show_merc_receb", False)
        # Ao abrir, limpe o checkbox ANTES dele ser criado neste run
        if st.session_state["show_merc_receb"]:
            if "merc_receb_confirma_out" in st.session_state:
                del st.session_state["merc_receb_confirma_out"]
        st.rerun()

    if not st.session_state.get("show_merc_receb", False):
        return

    st.markdown("#### 📥 Recebimento de Mercadorias")

    # Opcional: permitir listar tudo
    mostrar_todas = st.checkbox("Mostrar já recebidas", value=False, key="chk_merc_mostrar_todas")

    # Carrega compras (por padrão, só pendentes)
    try:
        with get_conn(caminho_banco) as conn:
            _ensure_extra_cols(conn)
            cur = conn.cursor()

            base_select = """
                SELECT id, Data, Colecao, Fornecedor,
                       Previsao_Faturamento, Previsao_Recebimento, Numero_Pedido,
                       Recebimento,
                       Valor_Mercadoria, Frete,
                       Numero_NF
                  FROM mercadorias
            """
            where_clause = "" if mostrar_todas else "WHERE Recebimento IS NULL OR TRIM(Recebimento) = ''"

            rows = cur.execute(f"""
                {base_select}
                {where_clause}
                ORDER BY date(Data) DESC, rowid DESC
                LIMIT 200
            """).fetchall()

            compras = [
                {
                    "id": r[0],
                    "Data": r[1] or "",
                    "Colecao": r[2] or "",
                    "Fornecedor": r[3] or "",
                    "PrevFat": r[4] or "",
                    "PrevRec": r[5] or "",
                    "Pedido": r[6],
                    "Recebimento": r[7],
                    "Valor_Mercadoria": float(r[8]) if r[8] is not None else 0.0,
                    "Frete": float(r[9]) if r[9] is not None else 0.0,
                    "Numero_Pedido": "" if r[6] is None else str(r[6]),
                    "Numero_NF": "" if r[10] is None else str(r[10]),
                } for r in rows
            ]
    except Exception as e:
        st.error(f"Erro ao carregar compras: {e}")
        return

    if not compras:
        st.info("Nenhuma compra pendente de recebimento.")
        return

    label_map = {
        c["id"]: f"#{c['id']} • {c['Data']} • {c['Fornecedor']} • {c['Colecao']} • Pedido:{c['Pedido']}"
        for c in compras
    }
    selected_id = st.selectbox(
        "Selecione a compra",
        options=list(label_map.keys()),
        format_func=lambda k: label_map[k],
        key="merc_receb_sel"
    )
    sel = next((c for c in compras if c["id"] == selected_id), None)
    if not sel:
        st.warning("Seleção inválida.")
        return

    with st.form("form_merc_receb"):
        # Linha 1 — cabeçalho bloqueado
        b1, b2, b3 = st.columns([1, 1, 1.4])
        with b1:
            st.text_input("Data da Compra", value=sel["Data"], disabled=True)
        with b2:
            st.text_input("Coleção", value=sel["Colecao"], disabled=True)
        with b3:
            st.text_input("Fornecedor", value=sel["Fornecedor"], disabled=True)

        # Linha 2 — previsões bloqueadas
        b4, b5 = st.columns(2)
        with b4:
            st.text_input("Previsão de Faturamento", value=sel["PrevFat"], disabled=True)
        with b5:
            st.text_input("Previsão de Recebimento", value=sel["PrevRec"], disabled=True)

        # Linha 3 — valores do pedido (somente leitura, formatados)
        v1, v2 = st.columns(2)
        with v1:
            st.text_input("Valor da Mercadoria (pedido)", value=formatar_valor(sel["Valor_Mercadoria"]), disabled=True)
        with v2:
            st.text_input("Frete (pedido)", value=formatar_valor(sel["Frete"]), disabled=True)

        # Linha 4 — permitir editar Nº Pedido e Nº NF
        n1, n2 = st.columns(2)
        with n1:
            numero_pedido_txt = st.text_input("Número do Pedido (editável)", value=sel["Numero_Pedido"], key="merc_receb_edit_pedido")
        with n2:
            numero_nf_txt = st.text_input("Número da Nota Fiscal (editável)", value=sel["Numero_NF"], key="merc_receb_edit_nf")

        st.markdown("###### Informe os dados efetivos e divergências (se houver)")
        e1, e2 = st.columns(2)
        with e1:
            fat_dt = st.date_input("Faturamento (efetivo)", value=data_lanc, key="merc_receb_fat_dt")
        with e2:
            rec_dt = st.date_input("Recebimento (efetivo)", value=data_lanc, key="merc_receb_rec_dt")

        d1, d2 = st.columns(2)
        with d1:
            valor_recebido = st.number_input("Valor Recebido (R$)", min_value=0.0, step=0.01, key="merc_receb_valor_recebido")
        with d2:
            frete_cobrado = st.number_input("Frete Cobrado (R$)", min_value=0.0, step=0.01, key="merc_receb_frete_cobrado")

        obs = st.text_area(
            "Observações (divergências, avarias, diferenças de quantidade etc.)",
            key="merc_receb_obs",
            placeholder="Opcional"
        )

        submitted = st.form_submit_button(
            "💾 Salvar Recebimento",
            use_container_width=True,
            disabled=not st.session_state.get("merc_receb_confirma_out", False)
        )

    # ✅ Checkbox posicionado ABAIXO do formulário (visual)
    st.checkbox("Confirmo os dados", key="merc_receb_confirma_out")

    if not submitted:
        return

    # Validação server-side extra (segurança)
    if not st.session_state.get("merc_receb_confirma_out", False):
        st.warning("⚠️ Marque 'Confirmo os dados' para salvar.")
        return

    try:
        with get_conn(caminho_banco) as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE mercadorias
                   SET Faturamento = ?,
                       Recebimento = ?,
                       Valor_Recebido = ?,
                       Frete_Cobrado = ?,
                       Recebimento_Obs = ?,
                       Numero_Pedido = ?,
                       Numero_NF = ?
                 WHERE id = ?
            """, (
                str(fat_dt) if fat_dt else None,
                str(rec_dt) if rec_dt else None,
                _to_float_or_none(valor_recebido),
                _to_float_or_none(frete_cobrado),
                (obs or None),
                (numero_pedido_txt.strip() or None),
                (numero_nf_txt.strip() or None),
                int(selected_id)
            ))
            conn.commit()

        st.session_state["msg_ok"] = "✅ Recebimento registrado/atualizado com sucesso!"
        st.session_state["show_merc_receb"] = False
        st.rerun()

    except Exception as e:
        st.error(f"❌ Erro ao salvar recebimento: {e}")