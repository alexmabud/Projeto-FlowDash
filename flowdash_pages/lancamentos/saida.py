import streamlit as st
import pandas as pd
from datetime import date
from typing import Optional, List

from services.ledger import LedgerService
from repository.cartoes_repository import CartoesRepository, listar_destinos_fatura_em_aberto
from repository.categorias_repository import CategoriasRepository
from flowdash_pages.cadastros.cadastro_classes import BancoRepository
from shared.db import get_conn
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository

# ======================================================================================
# Constantes
# ======================================================================================

FORMAS = ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "BOLETO"]
ORIGENS_DINHEIRO = ["Caixa", "Caixa 2"]

# ======================================================================================
# Helpers gerais
# ======================================================================================

def _distinct_lower_trim(series: pd.Series) -> list[str]:
    if series is None or series.empty:
        return []
    df = pd.DataFrame({"orig": series.fillna("").astype(str).str.strip()})
    df["key"] = df["orig"].str.lower().str.strip()
    df = df[df["key"] != ""].drop_duplicates("key", keep="first")
    return df["orig"].sort_values().tolist()


def _opcoes_pagamentos(caminho_banco: str, tipo: str) -> List[str]:
    """
    Retorna destinos para Categoria=Pagamentos, filtrando apenas títulos EM ABERTO.
      - Fatura Cartão de Crédito: (NÃO USADO AQUI — usamos listar_destinos_fatura_em_aberto)
      - Boletos: credores com boletos em aberto (exclui cartões e empréstimos).
      - Empréstimos e Financiamentos: rótulo do banco/descrição/tipo do cadastro.
    """
    with get_conn(caminho_banco) as conn:
        if tipo == "Fatura Cartão de Crédito":
            return []

        elif tipo == "Empréstimos e Financiamentos":
            df_emp = pd.read_sql("""
                SELECT DISTINCT
                    TRIM(
                        COALESCE(
                            NULLIF(TRIM(banco),''),
                            NULLIF(TRIM(descricao),''),
                            NULLIF(TRIM(tipo),'')
                        )
                    ) AS rotulo
                FROM emprestimos_financiamentos
            """, conn)
            df_emp = df_emp.dropna()
            df_emp = df_emp[df_emp["rotulo"] != ""]
            return _distinct_lower_trim(df_emp["rotulo"]) if not df_emp.empty else []

        elif tipo == "Boletos":
            # 1) nomes de cartões (para excluir da lista de boletos)
            df_cart = pd.read_sql("""
                SELECT DISTINCT TRIM(nome) AS nome
                  FROM cartoes_credito
                 WHERE nome IS NOT NULL AND TRIM(nome) <> ''
            """, conn)
            cart_set = set(x.strip().lower() for x in (df_cart["nome"].dropna().tolist() if not df_cart.empty else []))

            # 2) rótulos de empréstimos (para excluir da lista de boletos)
            df_emp = pd.read_sql("""
                SELECT DISTINCT TRIM(
                    COALESCE(
                        NULLIF(TRIM(banco),''),
                        NULLIF(TRIM(descricao),''),
                        NULLIF(TRIM(tipo),'')
                    )
                ) AS rotulo
                  FROM emprestimos_financiamentos
            """, conn)
            emp_set = set(x.strip().lower() for x in (df_emp["rotulo"].dropna().tolist() if not df_emp.empty else []))

            # 3) credores que possuem títulos EM ABERTO em contas_a_pagar_mov
            df_cred = pd.read_sql("""
                SELECT DISTINCT TRIM(credor) AS credor
                  FROM contas_a_pagar_mov
                 WHERE credor IS NOT NULL AND TRIM(credor) <> ''
                   AND COALESCE(status, 'Em aberto') IN ('Em aberto', 'Parcial')
                 ORDER BY credor
            """, conn)

            def eh_boleto_nome(nm: str) -> bool:
                lx = (nm or "").strip().lower()
                return bool(lx) and (lx not in cart_set) and (lx not in emp_set)

            candidatos = [c for c in (df_cred["credor"].dropna().tolist() if not df_cred.empty else []) if eh_boleto_nome(c)]
            if not candidatos:
                return []
            df = pd.DataFrame({"rotulo": candidatos})
            df["key"] = df["rotulo"].str.lower().str.strip()
            df = df[df["key"] != ""].drop_duplicates("key", keep="first")
            return df["rotulo"].sort_values().tolist()

    return []


# ======================================================================================
# Página principal
# ======================================================================================

def render_saida(caminho_banco: str, data_lanc: date):
    with st.container():
        # Toggle do formulário
        if st.button("🔴 Saída", use_container_width=True, key="btn_saida_toggle"):
            st.session_state.form_saida = not st.session_state.get("form_saida", False)

        if not st.session_state.get("form_saida", False):
            return

        st.markdown("#### 📤 Lançar Saída")

        # Contexto do usuário
        usuario = st.session_state.get("usuario_logado", {"nome": "Sistema"})
        usuario_nome = usuario.get("nome", "Sistema")

        # Serviços / repos
        ledger = LedgerService(caminho_banco)
        bancos_repo = BancoRepository(caminho_banco)
        cartoes_repo = CartoesRepository(caminho_banco)
        cats_repo = CategoriasRepository(caminho_banco)
        cap_repo = ContasAPagarMovRepository(caminho_banco)  # mantido para integrações

        # Dados para selects
        df_bancos = bancos_repo.carregar_bancos()
        nomes_bancos = df_bancos["nome"].tolist() if not df_bancos.empty else []
        nomes_cartoes = cartoes_repo.listar_nomes()

        st.caption(f"Data do lançamento: **{data_lanc}**")

        # ===================== CAMPOS GERAIS =====================
        valor_saida = st.number_input("Valor da Saída", min_value=0.0, step=0.01, format="%.2f", key="valor_saida")
        forma_pagamento = st.selectbox("Forma de Pagamento", FORMAS, key="forma_pagamento_saida")

        # ===================== CATEGORIA / SUBCATEGORIA / PAGAMENTOS =====================
        df_cat = cats_repo.listar_categorias()
        if not df_cat.empty:
            cat_nome = st.selectbox("Categoria", df_cat["nome"].tolist(), key="categoria_saida")
            cat_id = int(df_cat[df_cat["nome"] == cat_nome].iloc[0]["id"])
        else:
            st.info("Dica: cadastre categorias em **Cadastro → 📂 Cadastro de Saídas**.")
            cat_nome = st.text_input("Categoria (digite)", key="categoria_saida_text")
            cat_id = None

        # flag case-insensitive para "Pagamentos"
        is_pagamentos = (cat_nome or "").strip().lower() == "pagamentos"

        subcat_nome = None
        tipo_pagamento_sel: Optional[str] = None
        destino_pagamento_sel: Optional[str] = None
        # >>> FATURA <<<
        competencia_fatura_sel: Optional[str] = None
        obrigacao_id_fatura: Optional[int] = None
        # ajustes de fatura
        multa_fatura = juros_fatura = desconto_fatura = 0.0
        # >>> BOLETO (pagamento de parcela) <<<
        parcela_boleto_escolhida: Optional[dict] = None
        multa_boleto = juros_boleto = desconto_boleto = 0.0
        # >>> EMPRESTIMO (pagamento de parcela) <<<
        parcela_emp_escolhida: Optional[dict] = None
        multa_emp = juros_emp = desconto_emp = 0.0

        if is_pagamentos:
            tipo_pagamento_sel = st.selectbox(
                "Tipo de Pagamento",
                ["Fatura Cartão de Crédito", "Empréstimos e Financiamentos", "Boletos"],
                key="tipo_pagamento_pagamentos"
            )

            # ===== Fatura Cartão =====
            if tipo_pagamento_sel == "Fatura Cartão de Crédito":
                faturas = listar_destinos_fatura_em_aberto(caminho_banco)  # [{label, cartao, competencia, obrigacao_id, saldo}, ...]
                opcoes = [f["label"] for f in faturas]
                escolha = st.selectbox("Fatura (cartão • mês • saldo)", opcoes, key="destino_fatura_comp")
                if escolha:
                    f_sel = next(f for f in faturas if f["label"] == escolha)
                    destino_pagamento_sel = f_sel["cartao"]
                    competencia_fatura_sel = f_sel["competencia"]
                    obrigacao_id_fatura = int(f_sel["obrigacao_id"])
                    st.caption(f"Selecionado: {destino_pagamento_sel} — {competencia_fatura_sel} • obrigação #{obrigacao_id_fatura}")

                    st.number_input(
                        "Valor do pagamento (pode ser parcial)",
                        value=float(valor_saida),
                        step=0.01,
                        format="%.2f",
                        disabled=True,
                        key="valor_pagamento_fatura_ro",
                        help="Este valor vem de 'Valor da Saída'. Para alterar, edite o campo acima."
                    )
                    colf1, colf2, colf3 = st.columns(3)
                    with colf1:
                        multa_fatura = st.number_input("Multa (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="multa_fatura")
                    with colf2:
                        juros_fatura = st.number_input("Juros (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="juros_fatura")
                    with colf3:
                        desconto_fatura = st.number_input("Desconto (−)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="desconto_fatura")

                    total_saida_fatura = float(valor_saida) + float(multa_fatura) + float(juros_fatura) - float(desconto_fatura)
                    st.caption(f"Total da saída (caixa/banco): R$ {total_saida_fatura:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

            # ===== Boletos =====
            elif tipo_pagamento_sel == "Boletos":
                # 1) Selecionar o CREDOR (destino)
                destinos = _opcoes_pagamentos(caminho_banco, "Boletos")
                destino_pagamento_sel = (
                    st.selectbox("Credor", destinos, key="destino_pagamentos")
                    if destinos else
                    st.text_input("Credor (digite)", key="destino_pagamentos_text")
                )

                if destino_pagamento_sel and str(destino_pagamento_sel).strip():
                    # 2) Carregar PARCELAS em aberto/parcial desse credor
                    with get_conn(caminho_banco) as conn:
                        df_parc = cap_repo.listar_boletos_em_aberto_detalhado(conn, destino_pagamento_sel)

                    if df_parc.empty:
                        st.info("Nenhuma parcela em aberto para este credor.")
                    else:
                        # Monta opções
                        def fmt_row(r):
                            vcto = (r["vencimento"] or "")[:10]
                            parc = f"{int(r['parcela_num'])}/{int(r['parcelas_total'])}" if pd.notna(r["parcela_num"]) and pd.notna(r["parcelas_total"]) else "-"
                            saldo = float(r["saldo"] or 0.0)
                            return f"#{int(r['obrigacao_id'])} • Parcela {parc} • Venc. {vcto} • Saldo R$ {saldo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

                        df_parc = df_parc.copy()
                        df_parc["op"] = df_parc.apply(fmt_row, axis=1)
                        escolha_parc = st.selectbox("Parcela do Boleto", df_parc["op"].tolist(), key="parcela_boleto_op")

                        if escolha_parc:
                            r = df_parc[df_parc["op"] == escolha_parc].iloc[0]
                            parcela_boleto_escolhida = {
                                "obrigacao_id": int(r["obrigacao_id"]),
                                "saldo": float(r["saldo"] or 0.0),
                                "parcela_num": int(r["parcela_num"] or 0),
                                "parcelas_total": int(r["parcelas_total"] or 0),
                                "vencimento": (r["vencimento"] or "")[:10],
                                "credor": r["credor"],
                                "descricao": r["descricao"],
                            }
                            st.caption(f"Selecionado: obrigação #{parcela_boleto_escolhida['obrigacao_id']} • Parcela {parcela_boleto_escolhida['parcela_num']}/{parcela_boleto_escolhida['parcelas_total']} • Venc. {parcela_boleto_escolhida['vencimento']}")

                            st.number_input(
                                "Valor do pagamento (pode ser parcial)",
                                value=float(valor_saida),
                                step=0.01,
                                format="%.2f",
                                disabled=True,
                                key="valor_pagamento_boleto_ro",
                                help="Este valor vem de 'Valor da Saída'. Para alterar, edite o campo acima."
                            )
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                multa_boleto = st.number_input("Multa (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="multa_boleto")
                            with col2:
                                juros_boleto = st.number_input("Juros (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="juros_boleto")
                            with col3:
                                desconto_boleto = st.number_input("Desconto (−)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="desconto_boleto")

                            total_saida_calc = float(valor_saida) + float(multa_boleto) + float(juros_boleto) - float(desconto_boleto)
                            st.caption(f"Total da saída (caixa/banco): R$ {total_saida_calc:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

            # ===== Empréstimos e Financiamentos =====
            else:
                # 1) Selecionar o destino (banco/descrição do empréstimo)
                destinos = _opcoes_pagamentos(caminho_banco, "Empréstimos e Financiamentos")
                destino_pagamento_sel = st.selectbox(
                    "Selecione o Banco/Descrição do Empréstimo",
                    options=destinos,
                    index=0 if destinos else None,
                    key="destino_pagamentos_emprestimo"
                )

                # 2) Listar parcelas com saldo em aberto (cada parcela = um obrigacao_id)
                if destino_pagamento_sel and str(destino_pagamento_sel).strip():
                    with get_conn(caminho_banco) as conn:
                        df_emp = pd.read_sql(
                            """
                            SELECT obrigacao_id, credor, descricao, competencia, vencimento,
                                total_lancado, total_pago, saldo_aberto
                            FROM (
                                SELECT
                                    l.obrigacao_id,
                                    l.credor,
                                    l.descricao,
                                    l.competencia,
                                    l.vencimento,
                                    COALESCE(l.valor_evento,0) AS total_lancado,

                                    -- total pago (somamos eventos PAGAMENTO que entram negativos)
                                    (
                                        SELECT COALESCE(
                                                SUM(CASE
                                                        WHEN UPPER(COALESCE(p.categoria_evento,'')) LIKE 'PAGAMENTO%'
                                                            THEN -p.valor_evento
                                                        ELSE 0
                                                    END), 0
                                            )
                                        FROM contas_a_pagar_mov p
                                        WHERE p.obrigacao_id = l.obrigacao_id
                                    ) AS total_pago,

                                    -- saldo = lançamento - total_pago
                                    COALESCE(l.valor_evento,0) -
                                    (
                                        SELECT COALESCE(
                                                SUM(CASE
                                                        WHEN UPPER(COALESCE(p.categoria_evento,'')) LIKE 'PAGAMENTO%'
                                                            THEN -p.valor_evento
                                                        ELSE 0
                                                    END), 0
                                            )
                                        FROM contas_a_pagar_mov p
                                        WHERE p.obrigacao_id = l.obrigacao_id
                                    ) AS saldo_aberto

                                FROM contas_a_pagar_mov l
                                WHERE l.categoria_evento = 'LANCAMENTO'
                                AND l.tipo_obrigacao   = 'EMPRESTIMO'
                                AND LOWER(TRIM(l.credor)) = LOWER(TRIM(?))
                                AND COALESCE(l.status,'Em aberto') IN ('Em aberto','Parcial')
                            ) t
                            WHERE COALESCE(saldo_aberto,0) > 0.004    -- evita ruído de arredondamento
                            ORDER BY DATE(vencimento) ASC, obrigacao_id ASC
                            """,
                            conn,
                            params=(destino_pagamento_sel,),
                        )

                    if df_emp.empty:
                        st.info("Nenhuma parcela em aberto para este empréstimo.")
                    else:
                        def fmt_row_e(r):
                            vcto = (r["vencimento"] or "")[:10]
                            saldo = float(r["saldo_aberto"] or 0.0)
                            lab = f"#{int(r['obrigacao_id'])} • {vcto} • {r['descricao']} • Saldo R$ {saldo:,.2f}"
                            return lab.replace(",", "X").replace(".", ",").replace("X", ".")

                        opcoes = {fmt_row_e(r): int(r["obrigacao_id"]) for _, r in df_emp.iterrows()}
                        escolha = st.selectbox("Escolha a parcela do empréstimo", list(opcoes.keys()), key="parcela_emp_op")
                        if escolha:
                            parcela_emp_escolhida = {"obrigacao_id": opcoes[escolha], "rotulo": escolha}

                # 3) Entradas de ajustes (espelha Valor da Saída)
                st.number_input(
                    "Valor do pagamento (pode ser parcial)",
                    value=float(valor_saida),
                    step=0.01,
                    format="%.2f",
                    disabled=True,
                    key="valor_pagamento_emp_ro",
                    help="Este valor vem de 'Valor da Saída'. Para alterar, edite o campo acima."
                )
                colE1, colE2, colE3 = st.columns(3)
                with colE1:
                    multa_emp = st.number_input("Multa (R$)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="multa_emp")
                with colE2:
                    juros_emp = st.number_input("Juros (R$)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="juros_emp")
                with colE3:
                    desconto_emp = st.number_input("Desconto (R$)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="desconto_emp")

        else:
            if cat_id:
                df_sub = cats_repo.listar_subcategorias(cat_id)
                if not df_sub.empty:
                    subcat_nome = st.selectbox("Subcategoria", df_sub["nome"].tolist(), key="subcategoria_saida")
                else:
                    subcat_nome = st.text_input("Subcategoria (digite)", key="subcategoria_saida_text")
            else:
                subcat_nome = st.text_input("Subcategoria (digite)", key="subcategoria_saida_text")

        # ===================== CAMPOS CONDICIONAIS À FORMA =====================
        parcelas = 1
        cartao_escolhido = ""
        banco_escolhido = ""
        origem_dinheiro = ""
        venc_1: Optional[date] = None
        fornecedor = ""
        documento = ""

        if forma_pagamento == "CRÉDITO":
            parcelas = st.selectbox("Parcelas", list(range(1, 13)), key="parcelas_saida")
            if nomes_cartoes:
                cartao_escolhido = st.selectbox("Cartão de Crédito", nomes_cartoes, key="cartao_credito")
            else:
                st.warning("⚠️ Nenhum cartão de crédito cadastrado.")
                return

        elif forma_pagamento == "DINHEIRO":
            origem_dinheiro = st.selectbox("Origem do Dinheiro", ORIGENS_DINHEIRO, key="origem_dinheiro")

        elif forma_pagamento in ["PIX", "DÉBITO"]:
            if nomes_bancos:
                banco_escolhido = st.selectbox("Banco da Saída", nomes_bancos, key="banco_saida")
            else:
                banco_escolhido = st.text_input("Banco da Saída (digite)", key="banco_saida_text")

        elif forma_pagamento == "BOLETO":
            parcelas = st.selectbox("Parcelas", list(range(1, 37)), index=0, key="parcelas_boleto")
            venc_1 = st.date_input("Vencimento da 1ª parcela", value=date.today(), key="venc1_boleto")
            col_a, col_b = st.columns(2)
            with col_a:
                fornecedor = st.text_input("Fornecedor (opcional)", key="forn_boleto")
            with col_b:
                documento = st.text_input("Documento/Nº (opcional)", key="doc_boleto")

        descricao = st.text_input("Descrição (opcional)", key="descricao_saida")

        # Monta descrição final com meta de Pagamentos
        meta_tag = ""
        if is_pagamentos:
            tipo_txt = tipo_pagamento_sel or "-"
            dest_txt = (destino_pagamento_sel or "-").strip()
            meta_tag = f" [PAGAMENTOS: tipo={tipo_txt}; destino={dest_txt}]"
        descricao_final = (descricao or "").strip() + meta_tag

        # ===================== RESUMO =====================
        data_saida_str = data_lanc.strftime("%d/%m/%Y")
        linhas_md = [
            "**Confirme os dados da saída**",
            f"- **Data:** {data_saida_str}",
            f"- **Valor:** R$ {valor_saida:.2f}",
            f"- **Forma de pagamento:** {forma_pagamento}",
            f"- **Categoria:** {cat_nome or '—'}",
            (f"- **Subcategoria:** {subcat_nome or '—'}") if not is_pagamentos else (f"- **Tipo Pagamento:** {tipo_pagamento_sel or '—'}"),
            (f"- **Destino:** {destino_pagamento_sel or '—'}") if is_pagamentos else "",
            f"- **Descrição:** {descricao_final or 'N/A'}",
        ]
        if forma_pagamento == "CRÉDITO":
            linhas_md += [f"- **Parcelas:** {parcelas}x", f"- **Cartão de Crédito:** {cartao_escolhido or '—'}"]
        elif forma_pagamento == "DINHEIRO":
            linhas_md += [f"- **Origem do Dinheiro:** {origem_dinheiro or '—'}"]
        elif forma_pagamento in ["PIX", "DÉBITO"]:
            linhas_md += [f"- **Banco da Saída:** {banco_escolhido or '—'}"]
        elif forma_pagamento == "BOLETO":
            linhas_md += [
                f"- **Parcelas:** {parcelas}x",
                f"- **Vencimento 1ª Parcela:** {venc_1.strftime('%d/%m/%Y') if venc_1 else '—'}",
                f"- **Fornecedor:** {fornecedor or '—'}",
                f"- **Documento:** {documento or '—'}",
            ]
        st.info("\n".join([l for l in linhas_md if l != ""]))

        confirmar = st.checkbox("Está tudo certo com os dados acima?", key="confirmar_saida")

        # ===================== SALVAR =====================
        if st.button("💾 Salvar Saída", use_container_width=True, key="btn_salvar_saida"):
            # Validações gerais
            if forma_pagamento in ["PIX", "DÉBITO"] and not banco_escolhido:
                st.warning("⚠️ Selecione ou digite o banco da saída.")
                return
            if forma_pagamento == "DINHEIRO" and not origem_dinheiro:
                st.warning("⚠️ Informe a origem do dinheiro (Caixa/Caixa 2).")
                return
            if not confirmar:
                st.warning("⚠️ Confirme os dados antes de salvar.")
                return

            # Branch especial: Categoria=Pagamentos / Tipo=Boletos → pagar parcela
            if is_pagamentos and tipo_pagamento_sel == "Boletos":
                if not destino_pagamento_sel or not str(destino_pagamento_sel).strip():
                    st.warning("Selecione o credor do boleto.")
                    return
                if not parcela_boleto_escolhida:
                    st.warning("Selecione a parcela do boleto para pagar.")
                    return

                valor_digitado = float(valor_saida)
                multa_val = float(st.session_state.get("multa_boleto", multa_boleto))
                juros_val = float(st.session_state.get("juros_boleto", juros_boleto))
                desc_val  = float(st.session_state.get("desconto_boleto", desconto_boleto))

                if valor_digitado <= 0 and (multa_val + juros_val - desc_val) <= 0:
                    st.warning("Informe um valor de pagamento > 0 ou ajustes (multa/juros/desconto).")
                    return

                data_str = str(data_lanc)
                try:
                    origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else banco_escolhido
                    id_saida, id_mov, id_cap = ledger.pagar_parcela_boleto(
                        data=data_str,
                        valor=valor_digitado,
                        forma_pagamento=forma_pagamento,
                        origem=origem,
                        obrigacao_id=int(parcela_boleto_escolhida["obrigacao_id"]),
                        usuario=usuario_nome,
                        categoria="Boletos",
                        sub_categoria=subcat_nome,
                        descricao=descricao_final,
                        descricao_extra_cap=f"{destino_pagamento_sel} Parcela {parcela_boleto_escolhida['parcela_num']}/{parcela_boleto_escolhida['parcelas_total']}",
                        multa=multa_val,
                        juros=juros_val,
                        desconto=desc_val
                    )
                    st.session_state["msg_ok"] = (
                        f"✅ Pagamento de boleto registrado! Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
                    )
                    st.session_state.form_saida = False
                    st.rerun()

                except Exception as e:
                    st.error(f"Erro ao pagar boleto: {e}")
                return  # evita cair nas lógicas padrão abaixo

            # Branch especial: Categoria=Pagamentos / Tipo=Fatura → pagar fatura com ajustes
            if is_pagamentos and tipo_pagamento_sel == "Fatura Cartão de Crédito":
                if not obrigacao_id_fatura:
                    st.warning("Selecione uma fatura em aberto (cartão • mês • saldo).")
                    return

                valor_digitado = float(valor_saida)
                multa_val = float(st.session_state.get("multa_fatura", multa_fatura))
                juros_val = float(st.session_state.get("juros_fatura", juros_fatura))
                desc_val  = float(st.session_state.get("desconto_fatura", desconto_fatura))

                if valor_digitado <= 0 and (multa_val + juros_val - desc_val) <= 0:
                    st.warning("Informe um valor de pagamento > 0 ou ajustes (multa/juros/desconto).")
                    return

                data_str = str(data_lanc)
                try:
                    origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else banco_escolhido
                    id_saida, id_mov, id_cap = ledger.pagar_fatura_cartao(
                        data=data_str,
                        valor=valor_digitado,
                        forma_pagamento=forma_pagamento,
                        origem=origem,
                        obrigacao_id=int(obrigacao_id_fatura),
                        usuario=usuario_nome,
                        categoria="Fatura Cartão de Crédito",
                        sub_categoria=subcat_nome,
                        descricao=descricao_final,
                        multa=multa_val,
                        juros=juros_val,
                        desconto=desc_val
                    )
                    st.session_state["msg_ok"] = (
                        f"✅ Pagamento de fatura registrado! Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
                    )
                    st.session_state.form_saida = False
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao pagar fatura: {e}")
                return  # evita cair nas lógicas padrão abaixo

            # Branch especial: Categoria=Pagamentos / Tipo=Empréstimos → pagar parcela
            if is_pagamentos and tipo_pagamento_sel == "Empréstimos e Financiamentos":
                if not destino_pagamento_sel:
                    st.warning("Selecione o banco/descrição do empréstimo.")
                    return
                if not parcela_emp_escolhida:
                    st.warning("Selecione a parcela do empréstimo.")
                    return

                valor_digitado = float(valor_saida)
                multa_val = float(st.session_state.get("multa_emp", multa_emp))
                juros_val = float(st.session_state.get("juros_emp", juros_emp))
                desc_val  = float(st.session_state.get("desconto_emp", desconto_emp))

                if valor_digitado <= 0 and (multa_val + juros_val - desc_val) <= 0:
                    st.warning("Informe um valor de pagamento > 0 ou ajustes (multa/juros/desconto).")
                    return

                data_str = str(data_lanc)
                try:
                    origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else banco_escolhido
                    id_saida, id_mov, id_cap = ledger.pagar_parcela_emprestimo(
                        data=data_str,
                        valor=valor_digitado,
                        forma_pagamento=forma_pagamento,
                        origem=origem,
                        obrigacao_id=int(parcela_emp_escolhida["obrigacao_id"]),
                        usuario=usuario_nome,
                        categoria="Empréstimos e Financiamentos",
                        sub_categoria=subcat_nome,
                        descricao=descricao_final,
                        multa=multa_val,
                        juros=juros_val,
                        desconto=desc_val,
                    )
                    st.session_state["msg_ok"] = (
                        f"✅ Parcela de Empréstimo paga! Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
                    )
                    st.info(f"Destino classificado: {tipo_pagamento_sel} → {destino_pagamento_sel or '—'}")
                    st.session_state.form_saida = False
                    st.rerun()

                except Exception as e:
                    st.error(f"Erro ao pagar parcela de empréstimo: {e}")
                return  # evita cair nas lógicas padrão abaixo

            # Validações padrão (fora dos fluxos especiais acima)
            if float(valor_saida) <= 0:
                st.warning("⚠️ O valor deve ser maior que zero.")
                return

            # Validação específica para a categoria Pagamentos (fluxos padrão)
            if is_pagamentos:
                if not tipo_pagamento_sel:
                    st.warning("Selecione o tipo de pagamento (Fatura, Empréstimos ou Boletos).")
                    return
                if tipo_pagamento_sel != "Fatura Cartão de Crédito":
                    if not destino_pagamento_sel or not str(destino_pagamento_sel).strip():
                        st.warning("Selecione o destino correspondente ao tipo escolhido.")
                        return
                else:
                    if not obrigacao_id_fatura:
                        st.warning("Selecione uma fatura em aberto (cartão • mês • saldo).")
                        return

            categoria = (cat_nome or "").strip()
            sub_categoria = (subcat_nome or "").strip()
            data_str = str(data_lanc)

            # Args extras para o Ledger quando Categoria = Pagamentos (fluxo padrão)
            extra_args = {}
            if is_pagamentos:
                extra_args["pagamento_tipo"] = tipo_pagamento_sel
                extra_args["pagamento_destino"] = destino_pagamento_sel
                if tipo_pagamento_sel == "Fatura Cartão de Crédito" and obrigacao_id_fatura:
                    extra_args["obrigacao_id_fatura"] = int(obrigacao_id_fatura)
                    if competencia_fatura_sel:
                        extra_args["competencia_pagamento"] = competencia_fatura_sel

            try:
                if forma_pagamento == "DINHEIRO":
                    id_saida, id_mov = ledger.registrar_saida_dinheiro(
                        data=data_str,
                        valor=float(valor_saida),
                        origem_dinheiro=origem_dinheiro,
                        categoria=categoria,
                        sub_categoria=sub_categoria,
                        descricao=descricao_final,
                        usuario=usuario_nome,
                        **extra_args
                    )
                    st.session_state["msg_ok"] = (
                        "⚠️ Transação já registrada (idempotência)." if id_saida == -1
                        else f"✅ Saída em dinheiro registrada! ID saída: {id_saida} | Log: {id_mov}"
                    )

                elif forma_pagamento in ["PIX", "DÉBITO"]:
                    id_saida, id_mov = ledger.registrar_saida_bancaria(
                        data=data_str,
                        valor=float(valor_saida),
                        banco_nome=banco_escolhido,
                        forma=forma_pagamento,
                        categoria=categoria,
                        sub_categoria=sub_categoria,
                        descricao=descricao_final,
                        usuario=usuario_nome,
                        **extra_args
                    )
                    st.session_state["msg_ok"] = (
                        "⚠️ Transação já registrada (idempotência)." if id_saida == -1
                        else f"✅ Saída bancária ({forma_pagamento}) registrada! ID saída: {id_saida} | Log: {id_mov}"
                    )

                elif forma_pagamento == "CRÉDITO":
                    fc_vc = cartoes_repo.obter_por_nome(cartao_escolhido)
                    if not fc_vc:
                        st.error("Cartão não encontrado. Cadastre em 📇 Cartão de Crédito.")
                        return
                    vencimento, fechamento = fc_vc
                    ids_fatura, id_mov = ledger.registrar_saida_credito(
                        data_compra=data_str,
                        valor=float(valor_saida),
                        parcelas=int(parcelas),
                        cartao_nome=cartao_escolhido,
                        categoria=categoria,
                        sub_categoria=subcat_nome,
                        descricao=descricao_final,
                        usuario=usuario_nome,
                        fechamento=int(fechamento),
                        vencimento=int(vencimento)
                    )
                    st.session_state["msg_ok"] = (
                        "⚠️ Transação já registrada (idempotência)." if not ids_fatura
                        else f"✅ Despesa em CRÉDITO programada! Parcelas criadas: {len(ids_fatura)} | Log: {id_mov}"
                    )

                elif forma_pagamento == "BOLETO":
                    ids_cap, id_mov = ledger.registrar_saida_boleto(
                        data_compra=data_str,
                        valor=float(valor_saida),
                        parcelas=int(parcelas),
                        vencimento_primeira=str(venc_1),
                        categoria=categoria,
                        sub_categoria=sub_categoria,
                        descricao=descricao_final,
                        usuario=usuario_nome,
                        fornecedor=fornecedor or None,
                        documento=documento or None
                    )
                    st.session_state["msg_ok"] = (
                        "⚠️ Transação já registrada (idempotência)." if not ids_cap
                        else f"✅ Boleto programado! Parcelas criadas: {len(ids_cap)} | Log: {id_mov}"
                    )

                # Feedback de classificação para fluxos padrão
                if is_pagamentos and tipo_pagamento_sel != "Boletos":
                    st.info(f"Destino classificado: {tipo_pagamento_sel} → {destino_pagamento_sel or '—'}")

                st.session_state.form_saida = False
                st.rerun()

            except Exception as e:
                st.error(f"Erro ao salvar saída: {e}")