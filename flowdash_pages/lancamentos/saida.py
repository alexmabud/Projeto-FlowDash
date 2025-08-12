import streamlit as st
from datetime import date
from services.ledger import LedgerService
from repository.cartoes_repository import CartoesRepository
from repository.categorias_repository import CategoriasRepository
from flowdash_pages.cadastros.cadastro_classes import BancoRepository

FORMAS = ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "BOLETO"]
ORIGENS_DINHEIRO = ["Caixa", "Caixa 2"]

def render_saida(caminho_banco: str, data_lanc: date):
    with st.container():
        # Botão toggle do formulário (uma coluna, como você usa)
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

        # Apoio
        df_bancos = bancos_repo.carregar_bancos()
        nomes_bancos = df_bancos["nome"].tolist() if not df_bancos.empty else []
        nomes_cartoes = cartoes_repo.listar_nomes()

        st.caption(f"Data do lançamento: **{data_lanc}**")
        valor_saida = st.number_input("Valor da Saída", min_value=0.0, step=0.01, format="%.2f", key="valor_saida")
        forma_pagamento = st.selectbox("Forma de Pagamento", FORMAS, key="forma_pagamento_saida")

        # -------- Categoria/Subcategoria dinâmicas (do banco) --------
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

        # -------- Resumo no padrão de Nova Venda (visual apenas) --------
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

        if forma_pagamento == "CRÉDITO":
            linhas_md += [
                f"- **Parcelas:** {parcelas}x",
                f"- **Cartão de Crédito:** {cartao_escolhido or '—'}",
            ]
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

            categoria = (cat_nome or "").strip()
            sub_categoria = (subcat_nome or "").strip()
            data_str = str(data_lanc)

            try:
                if forma_pagamento == "DINHEIRO":
                    id_saida, id_mov = ledger.registrar_saida_dinheiro(
                        data=data_str,
                        valor=float(valor_saida),
                        origem_dinheiro=origem_dinheiro,
                        categoria=categoria,
                        sub_categoria=sub_categoria,
                        descricao=descricao,
                        usuario=usuario_nome,
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
                        descricao=descricao,
                        usuario=usuario_nome,
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