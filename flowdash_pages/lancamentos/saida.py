import streamlit as st
import pandas as pd  # <— novo
from datetime import date
from services.ledger import LedgerService
from repository.cartoes_repository import CartoesRepository
from repository.categorias_repository import CategoriasRepository
from flowdash_pages.cadastros.cadastro_classes import BancoRepository

from shared.db import get_conn
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository

FORMAS = ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "BOLETO"]
ORIGENS_DINHEIRO = ["Caixa", "Caixa 2"]

def render_saida(caminho_banco: str, data_lanc: date):
    with st.container():
        if st.button("🔴 Saída", use_container_width=True, key="btn_saida_toggle"):
            st.session_state.form_saida = not st.session_state.get("form_saida", False)

        if not st.session_state.get("form_saida", False):
            return

        st.markdown("#### 📤 Lançar Saída")

        usuario = st.session_state.get("usuario_logado", {"nome": "Sistema"})
        usuario_nome = usuario.get("nome", "Sistema")

        ledger = LedgerService(caminho_banco)
        bancos_repo = BancoRepository(caminho_banco)
        cartoes_repo = CartoesRepository(caminho_banco)
        cats_repo = CategoriasRepository(caminho_banco)
        cap_repo = ContasAPagarMovRepository(caminho_banco)

        # Apoio
        df_bancos = bancos_repo.carregar_bancos()
        nomes_bancos = df_bancos["nome"].tolist() if not df_bancos.empty else []
        nomes_cartoes = cartoes_repo.listar_nomes()

        st.caption(f"Data do lançamento: **{data_lanc}**")

        # ===================== CAMPOS GERAIS =====================
        valor_saida = st.number_input("Valor da Saída", min_value=0.0, step=0.01, format="%.2f", key="valor_saida")
        forma_pagamento = st.selectbox("Forma de Pagamento", FORMAS, key="forma_pagamento_saida")

        # -------- Categoria/Subcategoria --------
        df_cat = cats_repo.listar_categorias()
        if not df_cat.empty:
            cat_nome = st.selectbox("Categoria", df_cat["nome"].tolist(), key="categoria_saida")
            cat_id = int(df_cat[df_cat["nome"] == cat_nome].iloc[0]["id"])
            df_sub = cats_repo.listar_subcategorias(cat_id)
            if not df_sub.empty:
                subcat_nome = st.selectbox("Subcategoria", df_sub["nome"].tolist(), key="subcategoria_saida")
            else:
                subcat_nome = st.text_input("Subcategoria (digite)", key="subcategoria_saida_text")
        else:
            st.info("Dica: cadastre categorias em **Cadastro → 📂 Cadastro de Saídas**.")
            cat_nome = st.text_input("Categoria (digite)", key="categoria_saida_text")
            subcat_nome = st.text_input("Subcategoria (digite)", key="subcategoria_saida_text")

        # -------- Campos condicionais à forma --------
        parcelas = 1
        cartao_escolhido = ""
        banco_escolhido = ""
        origem_dinheiro = ""
        venc_1 = None
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

        # ===================== VINCULAR (AGORA EMBAIXO) =====================
        tipo_obrigacao_escolhido = None
        obrigacao_id_escolhido = None
        saldo_obrigacao = None

        pode_vincular = forma_pagamento in ["DINHEIRO", "PIX", "DÉBITO"]  # pagar obrigações
        if pode_vincular:
            st.write("**Vincular pagamento a uma obrigação (boleto/fatura/empréstimo)?**")
            vincular = st.checkbox("Vincular a BOLETO / FATURA_CARTAO / EMPRESTIMO", key="saida_vincular")

            if vincular:
                tipo_label = st.selectbox(
                    "Tipo de obrigação",
                    ["Fatura Cartão", "Empréstimo", "Boleto"],
                    key="saida_tipo_obrig_label"
                )
                tipo_map = {"Fatura Cartão": "FATURA_CARTAO", "Empréstimo": "EMPRESTIMO", "Boleto": "BOLETO"}
                tipo_obrigacao_escolhido = tipo_map[tipo_label]

                # Carrega obrigações em aberto desse tipo
                with get_conn(caminho_banco) as conn:
                    df_aberto = cap_repo.listar_em_aberto(conn, tipo_obrigacao_escolhido)

                if df_aberto.empty:
                    st.info("Nenhuma obrigação em aberto para esse tipo.")
                else:
                    # 1) Escolher pelo CREDOR (cartão/fornecedor/contrato) a partir da coluna 'credor'
                    op_credor = sorted(
                        [(c if pd.notna(c) and c else "(Sem credor)") for c in df_aberto["credor"].unique().tolist()]
                    )
                    credor_sel = st.selectbox("Escolha pelo nome (coluna 'credor')", op_credor, key="saida_credor")

                    df_f = df_aberto.copy()
                    df_f["credor_fix"] = df_f["credor"].fillna("(Sem credor)").replace("", "(Sem credor)")
                    df_f = df_f[df_f["credor_fix"] == credor_sel]

                    # 2) Para FATURA_CARTAO, pedir também o Mês (competência)
                    if tipo_obrigacao_escolhido == "FATURA_CARTAO":
                        meses = sorted([m for m in df_f["competencia"].dropna().unique().tolist() if m])
                        mes_atual = date.today().strftime("%Y-%m")
                        idx_default = meses.index(mes_atual) if mes_atual in meses else 0 if meses else 0
                        comp_sel = st.selectbox("Mês (competência)", meses, index=idx_default, key="saida_competencia")
                        df_f = df_f[df_f["competencia"] == comp_sel]

                    # 3) Escolher a obrigação específica
                    df_f = df_f.sort_values(by=["vencimento", "obrigacao_id"], na_position="last")
                    opcoes = []
                    for _, r in df_f.iterrows():
                        ven = (r.get("vencimento") or "")[:10]
                        desc = r.get("descricao") or credor_sel
                        saldo = float(r.get("saldo_aberto") or 0)
                        pct = float(r.get("perc_quitado") or 0)
                        label = f"[{int(r['obrigacao_id'])}] {desc}  |  venc {ven}  |  saldo R$ {saldo:,.2f}  |  {pct:.1f}%"
                        opcoes.append((int(r["obrigacao_id"]), label, saldo))

                    if opcoes:
                        labels = [o[1] for o in opcoes]
                        escolha = st.selectbox("Escolha a obrigação", labels, key="saida_obrig_label")
                        if escolha:
                            idx = labels.index(escolha)
                            obrigacao_id_escolhido, _, saldo_obrigacao = opcoes[idx]
                            # auto-preencher o valor quando a obrigação muda
                            if st.session_state.get("valor_saida_autofill_id") != obrigacao_id_escolhido:
                                st.session_state["valor_saida"] = float(saldo_obrigacao)
                                st.session_state["valor_saida_autofill_id"] = obrigacao_id_escolhido

        # -------- Resumo visual --------
        data_saida_str = data_lanc.strftime("%d/%m/%Y")
        linhas_md = [
            "**Confirme os dados da saída**",
            f"- **Data:** {data_saida_str}",
            f"- **Valor:** R$ {valor_saida:.2f}",
            f"- **Forma de pagamento:** {forma_pagamento}",
            f"- **Categoria:** {cat_nome or '—'}",
            f"- **Subcategoria:** {subcat_nome or '—'}",
            f"- **Descrição:** {descricao or 'N/A'}",
        ]
        if pode_vincular and st.session_state.get("saida_vincular") and obrigacao_id_escolhido:
            linhas_md.append(f"- **Vínculo:** {tipo_obrigacao_escolhido} #{obrigacao_id_escolhido} (saldo R$ {saldo_obrigacao:,.2f})")
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
        st.info("\n".join(linhas_md))

        confirmar = st.checkbox("Está tudo certo com os dados acima?", key="confirmar_saida")

        # -------- Salvar --------
        if st.button("💾 Salvar Saída", use_container_width=True, key="btn_salvar_saida"):
            if valor_saida <= 0:
                st.warning("⚠️ O valor deve ser maior que zero.")
                return
            if not confirmar:
                st.warning("⚠️ Confirme os dados antes de salvar.")
                return
            if forma_pagamento == "CRÉDITO" and not cartao_escolhido:
                st.warning("Selecione um cartão de crédito.")
                return
            if forma_pagamento in ["PIX", "DÉBITO"] and not banco_escolhido:
                st.warning("Selecione ou digite o banco da saída.")
                return
            if forma_pagamento == "DINHEIRO" and not origem_dinheiro:
                st.warning("Informe a origem do dinheiro (Caixa/Caixa 2).")
                return
            if forma_pagamento == "BOLETO" and not venc_1:
                st.warning("Informe o vencimento da 1ª parcela.")
                return
            if pode_vincular and st.session_state.get("saida_vincular") and not obrigacao_id_escolhido:
                st.warning("Selecione a obrigação a vincular.")
                return

            categoria = (cat_nome or "").strip()
            sub_categoria = (subcat_nome or "").strip()
            data_str = str(data_lanc)

            # Validação de saldo quando estiver vinculando
            if pode_vincular and st.session_state.get("saida_vincular") and obrigacao_id_escolhido:
                with get_conn(caminho_banco) as conn:
                    saldo = cap_repo.obter_saldo_obrigacao(conn, obrigacao_id_escolhido)
                if float(valor_saida) > max(0.0, float(saldo)):
                    st.warning(f"⚠️ Pagamento (R$ {valor_saida:.2f}) excede o saldo (R$ {float(saldo):.2f}).")
                    return

            try:
                vinculo = None
                if pode_vincular and st.session_state.get("saida_vincular") and obrigacao_id_escolhido:
                    vinculo = {
                        "obrigacao_id": int(obrigacao_id_escolhido),
                        "tipo_obrigacao": str(tipo_obrigacao_escolhido),
                        "valor_pagar": float(valor_saida),
                    }

                if forma_pagamento == "DINHEIRO":
                    id_saida, id_mov = ledger.registrar_saida_dinheiro(
                        data=data_str,
                        valor=float(valor_saida),
                        origem_dinheiro=origem_dinheiro,
                        categoria=categoria,
                        sub_categoria=sub_categoria,
                        descricao=descricao,
                        usuario=usuario_nome,
                        vinculo_pagamento=vinculo,
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
                        sub_categoria=subcategoria if (subcategoria := sub_categoria) else sub_categoria,
                        descricao=descricao,
                        usuario=usuario_nome,
                        vinculo_pagamento=vinculo,
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
                    fechamento, vencimento = fc_vc
                    ids_fatura, id_mov = ledger.registrar_saida_credito(
                        data_compra=data_str,
                        valor=float(valor_saida),
                        parcelas=int(parcelas),
                        cartao_nome=cartao_escolhido,
                        categoria=categoria,
                        sub_categoria=sub_categoria,
                        descricao=descricao,
                        usuario=usuario_nome,
                        fechamento=int(fechamento),
                        vencimento=int(vencimento),
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
                        descricao=descricao,
                        usuario=usuario_nome,
                        fornecedor=fornecedor or None,
                        documento=documento or None,
                    )
                    st.session_state["msg_ok"] = (
                        "⚠️ Transação já registrada (idempotência)." if not ids_cap
                        else f"✅ Boleto programado! Parcelas criadas: {len(ids_cap)} | Log: {id_mov}"
                    )

                st.session_state.form_saida = False
                st.rerun()

            except Exception as e:
                st.error(f"Erro ao salvar saída: {e}")