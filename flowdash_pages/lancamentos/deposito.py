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
    Deposita DINHEIRO do Caixa 2 em um banco:
      - Debita primeiro de 'caixa2_dia'; se faltar, debita o restante de 'caixa_2'.
      - Insere snapshot em 'saldos_caixas' com os novos saldos.
      - Lança UMA ENTRADA em 'movimentacoes_bancarias' (origem='deposito') para o banco escolhido.
      - Soma o valor no banco (coluna correspondente) em 'saldos_bancos' na MESMA data.
    """
    # Toggle do formulário
    if st.button("🏦 Depósito (Caixa 2 → Banco)", use_container_width=True, key="btn_deposito_toggle"):
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

    # Botão só habilita se confirmar
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
        trans_uid = str(uuid.uuid4())  # identificador único do lançamento no livro

        with get_conn(caminho_banco) as conn:
            cur = conn.cursor()

            # 1) Buscar último snapshot de saldos_caixas
            row = cur.execute("""
                SELECT data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
                  FROM saldos_caixas
              ORDER BY date(data) DESC, rowid DESC
                 LIMIT 1
            """).fetchone()

            # saldos atuais (ou zero se não houver snapshot)
            caixa        = _r2(float(row[1])) if row else 0.0
            caixa_2      = _r2(float(row[2])) if row else 0.0
            caixa_vendas = _r2(float(row[3])) if row else 0.0
            caixa2_dia   = _r2(float(row[5])) if row else 0.0

            # 2) Disponível no Caixa 2 (dia + saldo acumulado)
            caixa2_disp_total = _r2(caixa2_dia + caixa_2)
            if valor_f > caixa2_disp_total:
                st.warning(
                    f"⚠️ Valor indisponível no Caixa 2. Disponível: {formatar_valor(caixa2_disp_total)} "
                    f"(Dia: {formatar_valor(caixa2_dia)} • Saldo: {formatar_valor(caixa_2)})"
                )
                return

            # 3) Regra: abate primeiro de 'caixa2_dia', depois de 'caixa_2'
            usar_de_dia   = _r2(min(valor_f, caixa2_dia))
            usar_de_saldo = _r2(valor_f - usar_de_dia)

            novo_caixa2_dia = _r2(caixa2_dia - usar_de_dia)
            novo_caixa_2    = _r2(caixa_2 - usar_de_saldo)

            # clamps (não negativos)
            novo_caixa2_dia = max(0.0, novo_caixa2_dia)
            novo_caixa_2    = max(0.0, novo_caixa_2)

            # 4) Recalcular totais
            #    Caixa Total (dinheiro físico) não muda: envolve 'caixa' e 'caixa_vendas'
            novo_caixa_total  = _r2(caixa + caixa_vendas)
            #    Caixa 2 Total = novo_caixa_2 + novo_caixa2_dia
            novo_caixa2_total = _r2(novo_caixa_2 + novo_caixa2_dia)

            # 5) Gravar snapshot em saldos_caixas
            cur.execute("""
                INSERT INTO saldos_caixas
                    (data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total)
                VALUES (?,    ?,     ?,       ?,            ?,            ?,         ?)
            """, (
                data_str,
                caixa,                # inalterado
                novo_caixa_2,         # atualizado (↓ se houve uso)
                caixa_vendas,         # inalterado
                novo_caixa_total,     # recalculado (igual ao anterior)
                novo_caixa2_dia,      # atualizado (↓)
                novo_caixa2_total     # recalculado
            ))

            # 6) Lançar UMA entrada no livro (movimentacoes_bancarias)
            observ = (
                "Depósito (Caixa 2 → Banco) | "
                f"abatido: Caixa 2 (dia)={usar_de_dia:.2f}, Caixa 2 (saldo)={usar_de_saldo:.2f}"
            )
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco,    tipo,     valor,  origem,     observacao, referencia_id, referencia_tabela, trans_uid)
                VALUES (?,   ?,       ?,        ?,      ?,          ?,          ?,             ?,                 ?)
            """, (
                data_str, banco_nome, "entrada", valor_f,
                "deposito", observ,
                None, None, trans_uid
            ))

            conn.commit()

        # 7) Atualizar saldos_bancos (soma na coluna do banco, na mesma data)
        try:
            upsert_saldos_bancos(caminho_banco, data_str, banco_nome, valor_f)
        except Exception as e:
            # Se a tabela não existir ou o banco não estiver cadastrado, só avisa
            st.warning(f"Não foi possível atualizar saldos_bancos para '{banco_nome}': {e}")

        st.session_state["msg_ok"] = (
            f"✅ Depósito registrado em {banco_nome}: {formatar_valor(valor_f)} "
            f"(abatido do Caixa 2 — Dia: {formatar_valor(usar_de_dia)}, Saldo: {formatar_valor(usar_de_saldo)})."
        )
        st.session_state.form_deposito = False
        st.rerun()

    except Exception as e:
        st.error(f"❌ Erro ao registrar depósito: {e}")