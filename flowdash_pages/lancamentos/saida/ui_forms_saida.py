# ===================== UI Forms: Saída =====================
"""
Componentes de UI para Saída. Apenas interface – sem regra/SQL.
Mantém a mesma experiência do módulo original (campos e fluxos).
"""

from __future__ import annotations

import streamlit as st
import pandas as pd
from datetime import date
from typing import Optional, List

FORMAS = ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "BOLETO"]
ORIGENS_DINHEIRO = ["Caixa", "Caixa 2"]

def render_form_saida(
    data_lanc: date,
    invalidate_cb,
    nomes_bancos: list[str],
    nomes_cartoes: list[str],
    categorias_df,  # DataFrame com colunas (id, nome)
    listar_subcategorias_fn,  # fn(cat_id)->DataFrame
    listar_destinos_fatura_em_aberto_fn,  # fn()->list[dict]
    carregar_opcoes_pagamentos_fn,  # fn(tipo)->list[str]
):
    """
    Desenha o formulário de Saída e retorna um dicionário com todos os dados necessários.

    Args:
        data_lanc: Data do lançamento.
        invalidate_cb: callback para invalidar confirmação quando campos mudam.
        nomes_bancos: lista de bancos para PIX/DÉBITO.
        nomes_cartoes: lista de cartões para CRÉDITO.
        categorias_df: DataFrame de categorias (id, nome).
        listar_subcategorias_fn: função para obter subcategorias por categoria.
        listar_destinos_fatura_em_aberto_fn: função que retorna faturas em aberto.
        carregar_opcoes_pagamentos_fn: função que retorna opções para "Boletos" e "Empréstimos".

    Returns:
        dict com todos os campos preenchidos pelo usuário.
    """
    st.markdown("#### 📤 Lançar Saída")
    st.caption(f"Data do lançamento: **{data_lanc}**")

    # ===================== CAMPOS GERAIS =====================
    valor_saida = st.number_input(
        "Valor da Saída",
        min_value=0.0, step=0.01, format="%.2f",
        key="valor_saida", on_change=invalidate_cb
    )
    forma_pagamento = st.selectbox(
        "Forma de Pagamento", FORMAS,
        key="forma_pagamento_saida", on_change=invalidate_cb
    )

    # ===================== CATEGORIA / SUBCATEGORIA / PAGAMENTOS =====================
    if categorias_df is not None and not categorias_df.empty:
        cat_nome = st.selectbox(
            "Categoria", categorias_df["nome"].tolist(),
            key="categoria_saida", on_change=invalidate_cb
        )
        cat_id = int(categorias_df[categorias_df["nome"] == cat_nome].iloc[0]["id"])
    else:
        st.info("Dica: cadastre categorias em **Cadastro → 📂 Cadastro de Saídas**.")
        cat_nome = st.text_input("Categoria (digite)", key="categoria_saida_text")
        cat_id = None

    is_pagamentos = (cat_nome or "").strip().lower() == "pagamentos"

    # Campos usados no processamento
    subcat_nome = None
    tipo_pagamento_sel: Optional[str] = None
    destino_pagamento_sel: Optional[str] = None

    # >>> FATURA <<<
    competencia_fatura_sel: Optional[str] = None
    obrigacao_id_fatura: Optional[int] = None
    multa_fatura = juros_fatura = desconto_fatura = 0.0

    # >>> BOLETO <<<
    parcela_boleto_escolhida: Optional[dict] = None
    multa_boleto = juros_boleto = desconto_boleto = 0.0

    # >>> EMPRÉSTIMO <<<
    parcela_emp_escolhida: Optional[dict] = None
    multa_emp = juros_emp = desconto_emp = 0.0

    if is_pagamentos:
        tipo_pagamento_sel = st.selectbox(
            "Tipo de Pagamento",
            ["Fatura Cartão de Crédito", "Empréstimos e Financiamentos", "Boletos"],
            key="tipo_pagamento_pagamentos", on_change=invalidate_cb
        )

        # ===== Fatura Cartão =====
        if tipo_pagamento_sel == "Fatura Cartão de Crédito":
            faturas = listar_destinos_fatura_em_aberto_fn()  # [{label, cartao, competencia, obrigacao_id, saldo}, ...]
            opcoes = [f["label"] for f in faturas]
            escolha = st.selectbox("Fatura (cartão • mês • saldo)", opcoes, key="destino_fatura_comp", on_change=invalidate_cb)
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
            destinos = carregar_opcoes_pagamentos_fn("Boletos")
            destino_pagamento_sel = (
                st.selectbox("Credor", destinos, key="destino_pagamentos", on_change=invalidate_cb)
                if destinos else
                st.text_input("Credor (digite)", key="destino_pagamentos_text")
            )

            if destino_pagamento_sel and str(destino_pagamento_sel).strip():
                df_parc = st.session_state.get("_cap_boletos_df")  # opcional cache (você pode remover)
                # UI usa a mesma exibição que o módulo original; a busca real fica na action.
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

                # A seleção da parcela exata é feita via lookup na action (mantendo a lógica original)
                escolha_textual = st.text_input("Identificador/descrição da parcela (igual ao painel detalhado)", key="parcela_boleto_hint")

                total_saida_calc = float(valor_saida) + float(juros_boleto) + float(multa_boleto) - float(desconto_boleto)
                st.caption(f"Total da saída (caixa/banco): R$ {total_saida_calc:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

                # Passa apenas um hint textual (action faz o match real)
                parcela_boleto_escolhida = {"hint": (escolha_textual or "").strip()} if (escolha_textual or "").strip() else None

        # ===== Empréstimos e Financiamentos =====
        else:
            destinos = carregar_opcoes_pagamentos_fn("Empréstimos e Financiamentos")
            destino_pagamento_sel = st.selectbox(
                "Selecione o Banco/Descrição do Empréstimo",
                options=destinos,
                index=0 if destinos else None,
                key="destino_pagamentos_emprestimo",
                on_change=invalidate_cb
            )

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

            # seleção da parcela feita na action via lookup
            escolha_emp = st.text_input("Identificador/descrição da parcela do empréstimo (igual ao painel)", key="parcela_emp_hint")
            parcela_emp_escolhida = {"hint": (escolha_emp or "").strip()} if (escolha_emp or "").strip() else None

    else:
        # Subcategoria comum
        if cat_id:
            df_sub = listar_subcategorias_fn(cat_id)
            if df_sub is not None and not df_sub.empty:
                subcat_nome = st.selectbox("Subcategoria", df_sub["nome"].tolist(), key="subcategoria_saida", on_change=invalidate_cb)
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
        parcelas = st.selectbox("Parcelas", list(range(1, 13)), key="parcelas_saida", on_change=invalidate_cb)
        if nomes_cartoes:
            cartao_escolhido = st.selectbox("Cartão de Crédito", nomes_cartoes, key="saida_cartao_credito", on_change=invalidate_cb)
        else:
            st.warning("⚠️ Nenhum cartão de crédito cadastrado.")

    elif forma_pagamento == "DINHEIRO":
        origem_dinheiro = st.selectbox("Origem do Dinheiro", ORIGENS_DINHEIRO, key="origem_dinheiro", on_change=invalidate_cb)

    elif forma_pagamento in ["PIX", "DÉBITO"]:
        if nomes_bancos:
            banco_escolhido = st.selectbox("Banco da Saída", nomes_bancos, key="saida_banco_saida", on_change=invalidate_cb)
        else:
            banco_escolhido = st.text_input("Banco da Saída (digite)", key="saida_banco_saida_text", on_change=invalidate_cb)

    elif forma_pagamento == "BOLETO":
        parcelas = st.selectbox("Parcelas", list(range(1, 37)), index=0, key="parcelas_boleto", on_change=invalidate_cb)
        venc_1 = st.date_input("Vencimento da 1ª parcela", value=date.today(), key="venc1_boleto")
        col_a, col_b = st.columns(2)
        with col_a:
            fornecedor = st.text_input("Fornecedor (opcional)", key="forn_boleto")
        with col_b:
            documento = st.text_input("Documento/Nº (opcional)", key="doc_boleto")

    descricao = st.text_input("Descrição (opcional)", key="descricao_saida")

    # Monta descrição final com meta de Pagamentos (evita espaço duplo)
    meta_tag = ""
    if is_pagamentos:
        tipo_txt = tipo_pagamento_sel or "-"
        dest_txt = (destino_pagamento_sel or "-").strip()
        meta_tag = f"[PAGAMENTOS: tipo={tipo_txt}; destino={dest_txt}]"
    descricao_final = " ".join([(descricao or "").strip(), meta_tag]).strip()

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
        linhas_md += [f"- **Banco da Saída:** {(banco_escolhido or '').strip() or '—'}"]
    elif forma_pagamento == "BOLETO":
        linhas_md += [
            f"- **Parcelas:** {parcelas}x",
            f"- **Vencimento 1ª Parcela:** {venc_1.strftime('%d/%m/%Y') if venc_1 else '—'}",
            f"- **Fornecedor:** {fornecedor or '—'}",
            f"- **Documento:** {documento or '—'}",
        ]
    st.info("\n".join([l for l in linhas_md if l != ""]))

    confirmado = st.checkbox("Está tudo certo com os dados acima?", key="confirmar_saida")

    return {
        "valor_saida": float(valor_saida or 0.0),
        "forma_pagamento": forma_pagamento,
        "cat_nome": (cat_nome or "").strip(),
        "cat_id": cat_id,
        "subcat_nome": (subcat_nome or "").strip() if subcat_nome else None,
        "is_pagamentos": bool(is_pagamentos),
        "tipo_pagamento_sel": (tipo_pagamento_sel or "").strip() if is_pagamentos else None,
        "destino_pagamento_sel": (destino_pagamento_sel or "").strip() if is_pagamentos else None,
        "competencia_fatura_sel": competencia_fatura_sel,
        "obrigacao_id_fatura": obrigacao_id_fatura,
        "multa_fatura": float(multa_fatura),
        "juros_fatura": float(juros_fatura),
        "desconto_fatura": float(desconto_fatura),
        "parcela_boleto_escolhida": parcela_boleto_escolhida,
        "multa_boleto": float(multa_boleto),
        "juros_boleto": float(juros_boleto),
        "desconto_boleto": float(desconto_boleto),
        "parcela_emp_escolhida": parcela_emp_escolhida,
        "multa_emp": float(multa_emp),
        "juros_emp": float(juros_emp),
        "desconto_emp": float(desconto_emp),
        "parcelas": int(parcelas or 1),
        "cartao_escolhido": (cartao_escolhido or "").strip(),
        "banco_escolhido": (banco_escolhido or "").strip(),
        "origem_dinheiro": (origem_dinheiro or "").strip(),
        "venc_1": venc_1,
        "fornecedor": (fornecedor or "").strip(),
        "documento": (documento or "").strip(),
        "descricao_final": descricao_final,
        "confirmado": bool(confirmado),
    }