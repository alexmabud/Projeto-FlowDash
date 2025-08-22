"""
Módulo Depósito (Lançamentos)
=============================

Este módulo define a página e a lógica para registrar **depósitos bancários** 
no sistema FlowDash. Ele é usado para contabilizar valores em espécie que são 
transferidos do caixa físico para uma conta bancária, mantendo consistência 
no controle de saldos.

Funcionalidades principais
--------------------------
- Registro de depósitos vinculados a um banco específico.
- Ajuste automático:
  - Saída do valor no **Caixa** ou **Caixa 2**.
  - Entrada do valor no **Banco de destino**.
- Registro da movimentação em `movimentacoes_bancarias` via `LedgerService`.
- Interface em Streamlit com formulário simples (valor, banco de destino, data).

Detalhes técnicos
-----------------
- Implementado em Streamlit.
- Operação neutra no fluxo global de caixa (não cria entrada de receita),
  apenas movimenta recursos entre caixa físico e bancos.
- Garantia de idempotência através do `LedgerService`.
- Pode ser expandido para suportar depósitos oriundos de aportes/financiamentos.

Dependências
------------
- streamlit
- pandas
- datetime
- services.ledger.LedgerService
- repository.movimentacoes_repository.MovimentacoesRepository
- flowdash_pages.cadastros.cadastro_classes.BancoRepository
- shared.db.get_conn

"""

import uuid
import streamlit as st
import pandas as pd
from shared.db import get_conn
from utils.utils import formatar_valor
from flowdash_pages.cadastros.cadastro_classes import BancoRepository
from .shared_ui import upsert_saldos_bancos, canonicalizar_banco  # helpers padronizados

def _r2(x) -> float:
    """Arredonda em 2 casas (evita -0,00)."""
    return round(float(x or 0.0), 2)

def render_deposito(caminho_banco: str, data_lanc):
    """
    Depósito: debita do Caixa 2 (primeiro do dia, depois do saldo) e credita no banco escolhido.
    - saldos_caixas: UPSERT por dia (uma linha por dia).
    - movimentacoes_bancarias: **uma linha por lançamento** (sem agregar).
      -> referencia_id recebe o próprio id da linha (self-reference)
      -> referencia_tabela = 'movimentacoes_bancarias'
    - saldos_bancos: soma na coluna do banco na mesma data (helper upsert_saldos_bancos).
    """
    # Toggle do formulário
    if st.button("🏦 Depósito Bancário", use_container_width=True, key="btn_deposito_toggle"):
        st.session_state.form_deposito = not st.session_state.get("form_deposito", False)
        if st.session_state.form_deposito:
            st.session_state["deposito_confirmar"] = False  # sempre reinicia desmarcado

    if not st.session_state.get("form_deposito", False):
        return

    st.markdown("#### 🏦 Depósito de Caixa 2 no Banco")
    st.caption(f"Data do lançamento: **{pd.to_datetime(data_lanc).strftime('%d/%m/%Y')}**")

    # Bancos cadastrados
    bancos_repo = BancoRepository(caminho_banco)
    df_bancos = bancos_repo.carregar_bancos()
    nomes_bancos = df_bancos["nome"].tolist() if not df_bancos.empty else []

    col_a, col_b = st.columns(2)
    with col_a:
        valor = st.number_input("Valor do Depósito", min_value=0.0, step=0.01, format="%.2f", key="deposito_valor")
    with col_b:
        banco_escolhido = (
            st.selectbox("Banco de Destino", nomes_bancos, key="deposito_banco")
            if nomes_bancos else
            st.text_input("Banco de Destino (digite)", key="deposito_banco_text")
        )

    # Resumo
    st.info("\n".join([
        "**Confirme os dados do depósito**",
        f"- **Data:** {pd.to_datetime(data_lanc).strftime('%d/%m/%Y')}",
        f"- **Valor:** {formatar_valor(valor or 0.0)}",
        f"- **Banco de destino:** {(banco_escolhido or '—')}",
        f"- **Origem do dinheiro:** Caixa 2 (primeiro do dia, depois saldo)",
    ]))

    # Confirmação obrigatória
    confirmar = st.checkbox("Confirmo os dados acima", key="deposito_confirmar")
    salvar_btn = st.button("💾 Registrar Depósito", use_container_width=True, key="deposito_salvar", disabled=not confirmar)
    if not salvar_btn:
        return

    # 🔒 trava no servidor também
    if not st.session_state.get("deposito_confirmar", False):
        st.warning("⚠️ Confirme os dados antes de salvar.")
        return

    # Validações
    if (valor or 0.0) <= 0:
        st.warning("⚠️ Valor inválido.")
        return

    banco_in = (banco_escolhido or "").strip()
    if not banco_in:
        st.warning("⚠️ Selecione ou digite o banco de destino.")
        return

    # Canonicaliza o nome do banco (evita colunas “fantasma” em saldos_bancos)
    try:
        banco_nome = canonicalizar_banco(caminho_banco, banco_in) or banco_in
    except Exception:
        banco_nome = banco_in

    try:
        data_str = str(data_lanc)
        valor_f = _r2(valor)

        with get_conn(caminho_banco) as conn:
            cur = conn.cursor()

            # ================== SNAPSHOT DO DIA EM saldos_caixas (UPSERT) ==================
            df_caixas = pd.read_sql(
                "SELECT id, data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total FROM saldos_caixas",
                conn
            )
            snap_id = None
            base_caixa = base_caixa2 = base_vendas = base_caixa2dia = 0.0

            if not df_caixas.empty:
                df_caixas["data"] = pd.to_datetime(df_caixas["data"], errors="coerce", dayfirst=True)
                same_day = df_caixas[df_caixas["data"].dt.date == pd.to_datetime(data_lanc).date()]
                if not same_day.empty:
                    same_day = same_day.sort_values(["data","id"]).tail(1)
                    snap_id        = int(same_day.iloc[0]["id"])
                    base_caixa     = _r2(same_day.iloc[0].get("caixa", 0.0))
                    base_caixa2    = _r2(same_day.iloc[0].get("caixa_2", 0.0))
                    base_vendas    = _r2(same_day.iloc[0].get("caixa_vendas", 0.0))
                    base_caixa2dia = _r2(same_day.iloc[0].get("caixa2_dia", 0.0))
                else:
                    prev = df_caixas[df_caixas["data"].dt.date < pd.to_datetime(data_lanc).date()]
                    if not prev.empty:
                        prev = prev.sort_values(["data","id"]).tail(1)
                        base_caixa     = _r2(prev.iloc[0].get("caixa", 0.0))
                        base_caixa2    = _r2(prev.iloc[0].get("caixa_2", 0.0))
                        base_vendas    = _r2(prev.iloc[0].get("caixa_vendas", 0.0))
                        base_caixa2dia = 0.0  # novo dia começa com 0 no caixa2_dia

            base_total_cx2 = _r2(base_caixa2 + base_caixa2dia)
            if valor_f > base_total_cx2:
                st.warning(
                    f"⚠️ Valor indisponível no Caixa 2. Disponível: {formatar_valor(base_total_cx2)} "
                    f"(Dia: {formatar_valor(base_caixa2dia)} • Saldo: {formatar_valor(base_caixa2)})"
                )
                return

            # Debita primeiro do dia, depois do saldo
            usar_de_dia   = _r2(min(valor_f, base_caixa2dia))
            usar_de_saldo = _r2(valor_f - usar_de_dia)

            novo_caixa2_dia  = max(0.0, _r2(base_caixa2dia - usar_de_dia))
            novo_caixa_2     = max(0.0, _r2(base_caixa2 - usar_de_saldo))
            # Caixa físico não muda no depósito (vai do Caixa 2 pro banco)
            novo_caixa        = base_caixa
            novo_caixa_vendas = base_vendas
            novo_caixa_total  = _r2(novo_caixa + novo_caixa_vendas)
            novo_caixa2_total = _r2(novo_caixa_2 + novo_caixa2_dia)

            if snap_id is not None:
                cur.execute("""
                    UPDATE saldos_caixas
                       SET caixa=?,
                           caixa_2=?,
                           caixa_vendas=?,
                           caixa_total=?,
                           caixa2_dia=?,
                           caixa2_total=?
                     WHERE id=?
                """, (novo_caixa, novo_caixa_2, novo_caixa_vendas, novo_caixa_total,
                      novo_caixa2_dia, novo_caixa2_total, snap_id))
            else:
                cur.execute("""
                    INSERT INTO saldos_caixas
                        (data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total)
                    VALUES (?,    ?,     ?,       ?,            ?,           ?,          ?)
                """, (data_str, novo_caixa, novo_caixa_2, novo_caixa_vendas,
                      novo_caixa_total, novo_caixa2_dia, novo_caixa2_total))

            # ================== LIVRO: **uma linha por lançamento** ==================
            trans_uid = str(uuid.uuid4())
            observ = (
                f"Depósito Cx2→{banco_nome} | "
                f"Valor={formatar_valor(valor_f)} | "
                f"dia={usar_de_dia:.2f}; saldo={usar_de_saldo:.2f}"
            )
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco,   tipo,     valor,  origem,   observacao,
                     referencia_id, referencia_tabela, trans_uid)
                VALUES (?,   ?,      ?,        ?,      ?,        ?,
                        ?,             ?,                 ?)
            """, (
                data_str, banco_nome, "entrada", valor_f, "deposito", observ,
                None, "movimentacoes_bancarias", trans_uid
            ))
            mov_id = cur.lastrowid

            # Atualiza a própria linha com referencia_id = seu id e adiciona REF na observação
            observ_final = observ + f" | REF={mov_id}"
            cur.execute("""
                UPDATE movimentacoes_bancarias
                   SET referencia_id = ?, observacao = ?
                 WHERE id = ?
            """, (mov_id, observ_final, mov_id))

            conn.commit()

        # ================== saldos_bancos (helper padronizado) ==================
        try:
            upsert_saldos_bancos(caminho_banco, data_str, banco_nome, valor_f)
        except Exception as e:
            st.warning(f"Não foi possível atualizar saldos_bancos para '{banco_nome}': {e}")

        st.session_state["msg_ok"] = (
            f"✅ Depósito registrado em {banco_nome}: {formatar_valor(valor_f)} "
            f"(abatido do Caixa 2 — Dia: {formatar_valor(usar_de_dia)}, Saldo: {formatar_valor(usar_de_saldo)})."
        )
        st.session_state.form_deposito = False
        st.rerun()

    except Exception as e:
        st.error(f"❌ Erro ao registrar depósito: {e}")