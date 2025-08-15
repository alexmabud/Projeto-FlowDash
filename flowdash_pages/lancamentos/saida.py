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
      - Empréstimos e Financiamentos: como antes.
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
                   AND COALESCE(status, 'Em aberto') = 'Em aberto'
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
        cap_repo = ContasAPagarMovRepository(caminho_banco)  # mantido para futuras integrações

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
        # >>> NOVAS VARS PARA FATURA <<<
        competencia_fatura_sel: Optional[str] = None
        obrigacao_id_fatura: Optional[int] = None

        if is_pagamentos:
            tipo_pagamento_sel = st.selectbox(
                "Tipo de Pagamento",
                ["Fatura Cartão de Crédito", "Empréstimos e Financiamentos", "Boletos"],
                key="tipo_pagamento_pagamentos"
            )

            if tipo_pagamento_sel == "Fatura Cartão de Crédito":
                # lista faturas (cartão + competência + saldo) com obrigacao_id
                faturas = listar_destinos_fatura_em_aberto(caminho_banco)  # [{label, cartao, competencia, obrigacao_id, saldo}, ...]
                opcoes = [f["label"] for f in faturas]
                escolha = st.selectbox("Fatura (cartão • mês • saldo)", opcoes, key="destino_fatura_comp")
                if escolha:
                    f_sel = next(f for f in faturas if f["label"] == escolha)
                    destino_pagamento_sel = f_sel["cartao"]
                    competencia_fatura_sel = f_sel["competencia"]
                    obrigacao_id_fatura = int(f_sel["obrigacao_id"])
                    st.caption(f"Selecionado: {destino_pagamento_sel} — {competencia_fatura_sel} • obrigação #{obrigacao_id_fatura}")
                    # (Opcional) auto-preencher o valor com o saldo da fatura:
                    # st.session_state["valor_saida"] = f_sel["saldo"]
            else:
                # mantém lógica anterior para empréstimos/boletos
                destinos = _opcoes_pagamentos(caminho_banco, tipo_pagamento_sel)
                destino_pagamento_sel = (
                    st.selectbox("Destino", destinos, key="destino_pagamentos")
                    if destinos else
                    st.text_input("Destino (digite)", key="destino_pagamentos_text")
                )
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

            # Validação específica para a categoria Pagamentos
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

            # Args extras para o Ledger quando Categoria = Pagamentos
            extra_args = {}
            if is_pagamentos:
                extra_args["pagamento_tipo"] = tipo_pagamento_sel
                extra_args["pagamento_destino"] = destino_pagamento_sel
                # PRIORIDADE: se selecionou uma fatura específica, paga exatamente ela
                if tipo_pagamento_sel == "Fatura Cartão de Crédito" and obrigacao_id_fatura:
                    extra_args["obrigacao_id_fatura"] = int(obrigacao_id_fatura)
                    if competencia_fatura_sel:
                        extra_args["competencia_pagamento"] = competencia_fatura_sel  # opcional (descritivo/log)

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
                    # CartoesRepository.obter_por_nome => (vencimento, fechamento)
                    vencimento, fechamento = fc_vc
                    ids_fatura, id_mov = ledger.registrar_saida_credito(
                        data_compra=data_str,
                        valor=float(valor_saida),
                        parcelas=int(parcelas),
                        cartao_nome=cartao_escolhido,
                        categoria=categoria,
                        sub_categoria=subcat_nome,   # <- corrigido aqui
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

                # Feedback de classificação quando categoria = Pagamentos
                if is_pagamentos:
                    st.info(f"Destino classificado: {tipo_pagamento_sel} → {destino_pagamento_sel or '—'}")

                # Fecha o formulário e recarrega
                st.session_state.form_saida = False
                st.rerun()

            except Exception as e:
                st.error(f"Erro ao salvar saída: {e}")