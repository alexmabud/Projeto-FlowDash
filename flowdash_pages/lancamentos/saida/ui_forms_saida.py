# ===================== UI Forms: Saída =====================
"""
Componentes de UI para Saída. Apenas interface – sem regra/SQL.

Ajustes:
- Retorna sempre 'sub_categoria' como string (vazia se não preenchida).
- 'descricao' contém SOMENTE o que o usuário digitou (sem meta-tags).
- Em "Pagamentos", quando o campo descrição fica oculto, 'descricao' retorna "".
"""

from __future__ import annotations

from datetime import date
from typing import Optional, List, Callable, Any

import streamlit as st

__all__ = ["render_form_saida"]

FORMAS = ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "BOLETO"]
ORIGENS_DINHEIRO = ["Caixa", "Caixa 2"]


def render_form_saida(
    data_lanc: date,
    invalidate_cb: Optional[Callable[..., Any]],
    nomes_bancos: Optional[List[str]],
    nomes_cartoes: Optional[List[str]],
    categorias_df: Any,  # DataFrame esperado com colunas (id, nome)
    listar_subcategorias_fn: Optional[Callable[[int], Any]],  # fn(cat_id)->DataFrame
    listar_destinos_fatura_em_aberto_fn: Optional[Callable[[], List[dict]]],
    carregar_opcoes_pagamentos_fn: Optional[Callable[..., Any]] = None,  # legado (mantido)
    listar_boletos_em_aberto_fn: Optional[Callable[[], List[dict]]] = None,
    listar_empfin_em_aberto_fn: Optional[Callable[[], List[dict]]] = None,
) -> dict:
    st.markdown("#### 📤 Lançar Saída")
    st.caption(f"Data do lançamento: **{data_lanc}**")

    # ===================== CAMPOS GERAIS =====================
    valor_saida = st.number_input(
        "Valor da Saída",
        min_value=0.0,
        step=0.01,
        format="%.2f",
        key="valor_saida",
        on_change=(invalidate_cb if invalidate_cb else None),
    )
    forma_pagamento = st.selectbox(
        "Forma de Pagamento",
        FORMAS,
        key="forma_pagamento_saida",
        on_change=(invalidate_cb if invalidate_cb else None),
    )

    # ===================== CATEGORIA / SUBCATEGORIA / PAGAMENTOS =====================
    cat_nome: Optional[str] = None
    cat_id: Optional[int] = None

    if categorias_df is not None:
        try:
            if (not categorias_df.empty) and {"nome", "id"}.issubset(set(categorias_df.columns)):
                cat_nome = st.selectbox(
                    "Categoria",
                    categorias_df["nome"].tolist(),
                    key="categoria_saida",
                    on_change=(invalidate_cb if invalidate_cb else None),
                )
                if cat_nome:
                    cat_id = int(categorias_df.loc[categorias_df["nome"] == cat_nome, "id"].iloc[0])
            else:
                raise ValueError("categorias_df inválido")
        except Exception:
            cat_nome = st.text_input("Categoria (digite)", key="categoria_saida_text")
            cat_id = None
    else:
        st.info("Dica: cadastre categorias em **Cadastro → 📂 Cadastro de Saídas**.")
        cat_nome = st.text_input("Categoria (digite)", key="categoria_saida_text")
        cat_id = None

    is_pagamentos = (cat_nome or "").strip().lower() == "pagamentos"

    # Campos de processamento
    subcat_nome: Optional[str] = None
    tipo_pagamento_sel: Optional[str] = None
    destino_pagamento_sel: Optional[str] = None

    # >>> FATURA <<<
    competencia_fatura_sel: Optional[str] = None
    obrigacao_id_fatura: Optional[int] = None
    multa_fatura = juros_fatura = desconto_fatura = 0.0

    # >>> BOLETO <<<
    parcela_boleto_escolhida: Optional[dict] = None
    multa_boleto = juros_boleto = desconto_boleto = 0.0
    obrigacao_id_boleto: Optional[int] = None

    # >>> EMPRÉSTIMO <<<
    parcela_emp_escolhida: Optional[dict] = None
    multa_emp = juros_emp = desconto_emp = 0.0
    obrigacao_id_emprestimo: Optional[int] = None

    if is_pagamentos:
        tipo_pagamento_sel = st.selectbox(
            "Tipo de Pagamento",
            ["Fatura Cartão de Crédito", "Empréstimos e Financiamentos", "Boletos"],
            key="tipo_pagamento_pagamentos",
            on_change=(invalidate_cb if invalidate_cb else None),
        )

        # ---------- FATURA CARTÃO DE CRÉDITO ----------
        if tipo_pagamento_sel == "Fatura Cartão de Crédito":
            faturas = (listar_destinos_fatura_em_aberto_fn() if listar_destinos_fatura_em_aberto_fn else []) or []
            if not faturas:
                st.warning("Não há faturas de cartão em aberto.")
            else:
                opcoes = [f.get("label", "") for f in faturas]
                escolha = st.selectbox(
                    "Fatura (cartão • mês • saldo)",
                    opcoes,
                    key="destino_fatura_comp",
                    on_change=(invalidate_cb if invalidate_cb else None),
                )
                if escolha:
                    f_sel = next((f for f in faturas if f.get("label", "") == escolha), None)
                    if f_sel:
                        destino_pagamento_sel = f_sel.get("cartao", "")
                        competencia_fatura_sel = f_sel.get("competencia", "")
                        try:
                            obrigacao_id_fatura = int(f_sel.get("obrigacao_id"))
                        except Exception:
                            obrigacao_id_fatura = None

                        st.caption(
                            f"Selecionado: {destino_pagamento_sel} — {competencia_fatura_sel} • obrigação #{obrigacao_id_fatura or '—'}"
                        )

                        st.number_input(
                            "Valor do pagamento (pode ser parcial)",
                            value=float(valor_saida),
                            step=0.01,
                            format="%.2f",
                            disabled=True,
                            key="valor_pagamento_fatura_ro",
                            help="Este valor vem de 'Valor da Saída'. Para alterar, edite o campo acima.",
                        )
                        colf1, colf2, colf3 = st.columns(3)
                        with colf1:
                            multa_fatura = st.number_input(
                                "Multa (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="multa_fatura"
                            )
                        with colf2:
                            juros_fatura = st.number_input(
                                "Juros (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="juros_fatura"
                            )
                        with colf3:
                            desconto_fatura = st.number_input(
                                "Desconto (−)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="desconto_fatura"
                            )

                        total_saida_fatura = float(valor_saida) + float(multa_fatura) + float(juros_fatura) - float(desconto_fatura)
                        st.caption(
                            f"Total da saída (caixa/banco): R$ {total_saida_fatura:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                        )

        # ---------- EMPRÉSTIMOS E FINANCIAMENTOS ----------
        elif tipo_pagamento_sel == "Empréstimos e Financiamentos":
            itens = (listar_empfin_em_aberto_fn() if listar_empfin_em_aberto_fn else []) or []
            if not itens:
                st.warning("Não há parcelas de empréstimos/financiamentos em aberto.")
            else:
                opcoes = [i.get("label", "") for i in itens]
                escolha = st.selectbox(
                    "Selecione a parcela em aberto",
                    opcoes,
                    key="emp_parcela_em_aberto",
                    on_change=(invalidate_cb if invalidate_cb else None),
                )
                if escolha:
                    it = next((i for i in itens if i.get("label", "") == escolha), None)
                    if it:
                        destino_pagamento_sel = it.get("credor") or it.get("banco") or it.get("descricao") or ""
                        try:
                            obrigacao_id_emprestimo = int(it.get("obrigacao_id"))
                        except Exception:
                            obrigacao_id_emprestimo = None
                        parcela_emp_escolhida = it
                        st.caption(
                            f"Selecionado: {destino_pagamento_sel} • obrigação #{obrigacao_id_emprestimo or '—'}"
                            + (f" • parcela #{it.get('parcela_id')}" if it.get("parcela_id") else "")
                        )

                        st.number_input(
                            "Valor do pagamento (pode ser parcial)",
                            value=float(valor_saida),
                            step=0.01,
                            format="%.2f",
                            disabled=True,
                            key="valor_pagamento_emp_ro",
                            help="Este valor vem de 'Valor da Saída'. Para alterar, edite o campo acima.",
                        )
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            multa_emp = st.number_input("Multa (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="multa_emp")
                        with c2:
                            juros_emp = st.number_input("Juros (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="juros_emp")
                        with c3:
                            desconto_emp = st.number_input("Desconto (−)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="desconto_emp")

                        total_saida_emp = float(valor_saida) + float(multa_emp) + float(juros_emp) - float(desconto_emp)
                        st.caption(
                            f"Total da saída (caixa/banco): R$ {total_saida_emp:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                        )

        # ---------- BOLETOS ----------
        elif tipo_pagamento_sel == "Boletos":
            boletos = (listar_boletos_em_aberto_fn() if listar_boletos_em_aberto_fn else []) or []
            if not boletos:
                st.warning("Não há boletos em aberto.")
            else:
                opcoes = [b.get("label", "") for b in boletos]
                escolha = st.selectbox(
                    "Selecione o boleto/parcela em aberto",
                    opcoes,
                    key="boleto_em_aberto",
                    on_change=(invalidate_cb if invalidate_cb else None),
                )
                if escolha:
                    b = next((i for i in boletos if i.get("label", "") == escolha), None)
                    if b:
                        destino_pagamento_sel = b.get("credor") or b.get("descricao") or ""
                        try:
                            obrigacao_id_boleto = int(b.get("obrigacao_id"))
                        except Exception:
                            obrigacao_id_boleto = None
                        parcela_boleto_escolhida = b
                        st.caption(
                            f"Selecionado: {destino_pagamento_sel} • obrigação #{obrigacao_id_boleto or '—'}"
                            + (f" • parcela #{b.get('parcela_id')}" if b.get("parcela_id") else "")
                        )

                        st.number_input(
                            "Valor do pagamento (pode ser parcial)",
                            value=float(valor_saida),
                            step=0.01,
                            format="%.2f",
                            disabled=True,
                            key="valor_pagamento_boleto_ro",
                            help="Este valor vem de 'Valor da Saída'. Para alterar, edite o campo acima.",
                        )
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            multa_boleto = st.number_input("Multa (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="multa_boleto")
                        with col2:
                            juros_boleto = st.number_input("Juros (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="juros_boleto")
                        with col3:
                            desconto_boleto = st.number_input("Desconto (−)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="desconto_boleto")

                        total_saida_calc = float(valor_saida) + float(juros_boleto) + float(multa_boleto) - float(desconto_boleto)
                        st.caption(
                            f"Total da saída (caixa/banco): R$ {total_saida_calc:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                        )

    else:
        # Subcategoria comum (fora de Pagamentos)
        if cat_id and listar_subcategorias_fn:
            df_sub = listar_subcategorias_fn(cat_id)
            try:
                if df_sub is not None and (not df_sub.empty) and "nome" in df_sub.columns:
                    subcat_nome = st.selectbox(
                        "Subcategoria",
                        df_sub["nome"].tolist(),
                        key="subcategoria_saida",
                        on_change=(invalidate_cb if invalidate_cb else None),
                    )
                else:
                    subcat_nome = st.text_input("Subcategoria (digite)", key="subcategoria_saida_text")
            except Exception:
                subcat_nome = st.text_input("Subcategoria (digite)", key="subcategoria_saida_text")
        else:
            subcat_nome = st.text_input("Subcategoria (digite)", key="subcategoria_saida_text")

    # ===================== DESCRIÇÃO =====================
    esconder_descricao = bool(
        is_pagamentos and (tipo_pagamento_sel in ["Fatura Cartão de Crédito", "Empréstimos e Financiamentos", "Boletos"])
    )
    if not esconder_descricao:
        descricao_digitada = st.text_input("Descrição (opcional)", key="descricao_saida_generico", value="")
    else:
        descricao_digitada = ""

    # ===================== CAMPOS CONDICIONAIS À FORMA =====================
    parcelas = 1
    cartao_escolhido = ""
    banco_escolhido = ""
    origem_dinheiro = ""
    venc_1: Optional[date] = None
    credor_boleto = ""
    documento = ""  # compat

    if forma_pagamento == "CRÉDITO":
        parcelas = st.selectbox("Parcelas", list(range(1, 13)), key="parcelas_saida", on_change=(invalidate_cb if invalidate_cb else None))
        if nomes_cartoes:
            cartao_escolhido = st.selectbox("Cartão de Crédito", nomes_cartoes, key="saida_cartao_credito", on_change=(invalidate_cb if invalidate_cb else None))
        else:
            st.warning("⚠️ Nenhum cartão de crédito cadastrado.")
    elif forma_pagamento == "DINHEIRO":
        origem_dinheiro = st.selectbox("Origem do Dinheiro", ORIGENS_DINHEIRO, key="origem_dinheiro", on_change=(invalidate_cb if invalidate_cb else None))
    elif forma_pagamento in ["PIX", "DÉBITO"]:
        if nomes_bancos:
            banco_escolhido = st.selectbox("Banco da Saída", nomes_bancos, key="saida_banco_saida", on_change=(invalidate_cb if invalidate_cb else None))
        else:
            banco_escolhido = st.text_input("Banco da Saída (digite)", key="saida_banco_saida_text", on_change=(invalidate_cb if invalidate_cb else None))
    elif forma_pagamento == "BOLETO":
        parcelas = st.selectbox("Parcelas", list(range(1, 37)), index=0, key="parcelas_boleto", on_change=(invalidate_cb if invalidate_cb else None))
        venc_1 = st.date_input("Vencimento da 1ª parcela", value=date.today(), key="venc1_boleto")
        credor_boleto = st.text_input("Credor (Fornecedor)", key="credor_boleto")

    # ===================== RESUMO =====================
    data_saida_str = data_lanc.strftime("%d/%m/%Y")
    linhas_md = [
        "**Confirme os dados da saída**",
        f"- **Data:** {data_saida_str}",
        f"- **Valor:** R$ {valor_saida:.2f}",
        f"- **Forma de pagamento:** {forma_pagamento}",
        f"- **Categoria:** {cat_nome or '—'}",
        f"- **Subcategoria:** {(subcat_nome or '').strip() or '—'}" if not is_pagamentos else f"- **Tipo Pagamento:** {tipo_pagamento_sel or '—'}",
        (f"- **Destino:** {destino_pagamento_sel or '—'}") if is_pagamentos else "",
        f"- **Descrição:** {(descricao_digitada or '').strip() or 'N/A'}",
    ]
    st.info("\n".join([l for l in linhas_md if l != ""]))

    confirmado = st.checkbox("Está tudo certo com os dados acima?", key="confirmar_saida")
    st.info("Confirme os dados para habilitar o botão de salvar.")

    # --------- mapear 'credor' por forma ----------
    credor_val = ""
    if forma_pagamento == "CRÉDITO":
        credor_val = (cartao_escolhido or "").strip()
    elif forma_pagamento == "BOLETO":
        credor_val = (credor_boleto or "").strip()

    # ===== Retorno =====
    cat_safe = (cat_nome or "").strip()
    subcat_safe = (subcat_nome or "").strip() if subcat_nome else ""
    desc_safe = (descricao_digitada or "").strip()

    return {
        "valor_saida": float(valor_saida or 0.0),
        "forma_pagamento": forma_pagamento,

        # Categoria/Subcategoria (novos e ALIASES)
        "cat_nome": cat_safe,
        "cat_id": cat_id,
        "subcat_nome": subcat_safe,
        "categoria": cat_safe,          # alias legado
        "sub_categoria": subcat_safe,   # alias legado

        # Descrição — somente o que foi digitado
        "descricao_final": desc_safe,   # novo
        "descricao": desc_safe,         # alias legado

        # Pagamentos
        "is_pagamentos": bool(is_pagamentos),
        "tipo_pagamento_sel": (tipo_pagamento_sel or "").strip() if is_pagamentos else None,
        "destino_pagamento_sel": (destino_pagamento_sel or "").strip() if is_pagamentos else None,

        # Fatura
        "competencia_fatura_sel": competencia_fatura_sel,
        "obrigacao_id_fatura": obrigacao_id_fatura,
        "multa_fatura": float(multa_fatura),
        "juros_fatura": float(juros_fatura),
        "desconto_fatura": float(desconto_fatura),

        # Boletos
        "obrigacao_id_boleto": obrigacao_id_boleto,
        "parcela_boleto_escolhida": parcela_boleto_escolhida,
        "multa_boleto": float(multa_boleto),
        "juros_boleto": float(juros_boleto),
        "desconto_boleto": float(desconto_boleto),

        # Empréstimos
        "parcela_emp_escolhida": parcela_emp_escolhida,
        "multa_emp": float(multa_emp),
        "juros_emp": float(juros_emp),
        "desconto_emp": float(desconto_emp),
        "obrigacao_id_emprestimo": obrigacao_id_emprestimo,

        # Comuns/forma
        "parcelas": int(parcelas or 1),
        "cartao_escolhido": (cartao_escolhido or "").strip(),
        "banco_escolhido": (banco_escolhido or "").strip(),
        "origem_dinheiro": (origem_dinheiro or "").strip(),
        "venc_1": venc_1,

        # Credor para CAP
        "credor": credor_val,

        # Compat antigos
        "documento": "",
        "fornecedor": credor_boleto,

        "confirmado": bool(confirmado),
    }
