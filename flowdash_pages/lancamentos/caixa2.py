import streamlit as st
from .shared import get_conn
import uuid

def render_caixa2(caminho_banco: str, data_lanc):
    if st.button("🔄 Caixa 2", use_container_width=True, key="btn_caixa2_toggle"):
        st.session_state.form_caixa2 = not st.session_state.get("form_caixa2", False)
    if not st.session_state.get("form_caixa2", False):
        return

    st.markdown("#### 💸 Transferência para Caixa 2")
    valor = st.number_input("Valor a Transferir", min_value=0.0, step=0.01, key="caixa2_valor")
    confirmar = st.checkbox("Confirmo a transferência", key="caixa2_confirma")

    if st.button("💾 Confirmar Transferência", use_container_width=True, key="caixa2_salvar"):
        if valor <= 0:
            st.warning("⚠️ Valor inválido.")
            return
        if not confirmar:
            st.warning("⚠️ Confirme os dados antes de salvar.")
            return

        try:
            data_str = str(data_lanc)
            trans_uid = str(uuid.uuid4())   # precisa ser único pela restrição UNIQUE
            valor_f = float(valor)

            with get_conn(caminho_banco) as conn:
                cur = conn.cursor()

                # 1) Buscar último snapshot
                row = cur.execute("""
                    SELECT data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
                    FROM saldos_caixas
                    ORDER BY date(data) DESC, id DESC
                    LIMIT 1
                """).fetchone()

                caixa        = float(row[1]) if row else 0.0
                caixa_2      = float(row[2]) if row else 0.0
                caixa_vendas = float(row[3]) if row else 0.0
                caixa2_dia   = float(row[5]) if row else 0.0

                caixa_total_atual = caixa + caixa_vendas

                # 2) Validação: não transferir mais que o total disponível em dinheiro
                if valor_f > caixa_total_atual:
                    st.warning(f"⚠️ Valor indisponível. Caixa Total atual é R$ {caixa_total_atual:,.2f}.")
                    return

                # 3) Regra de prioridade: abate primeiro do 'caixa', depois de 'caixa_vendas'
                usar_de_caixa = min(valor_f, caixa)
                usar_de_vendas = valor_f - usar_de_caixa

                novo_caixa = caixa - usar_de_caixa
                novo_caixa_vendas = caixa_vendas - usar_de_vendas
                novo_caixa2_dia = caixa2_dia + valor_f

                # 4) Recalcular totais
                novo_caixa_total  = novo_caixa + novo_caixa_vendas
                novo_caixa2_total = caixa_2 + novo_caixa2_dia

                # 5) Gravar snapshot em saldos_caixas
                cur.execute("""
                    INSERT INTO saldos_caixas (data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    data_str,
                    novo_caixa,          # atualizado
                    caixa_2,             # inalterado
                    novo_caixa_vendas,   # atualizado
                    novo_caixa_total,    # recalculado
                    novo_caixa2_dia,     # atualizado (↑)
                    novo_caixa2_total    # recalculado
                ))

                # 6) UMA ÚNICA linha no 'livro': entrada em Caixa 2
                observ = (
                    f"Transferência recebida de dinheiro físico | "
                    f"abatido: Caixa={usar_de_caixa:.2f}, Caixa Vendas={usar_de_vendas:.2f}"
                )
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_id, referencia_tabela, trans_uid)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    data_str, "Caixa 2", "entrada", valor_f,
                    "transferencia_caixa", observ,
                    None, None, trans_uid
                ))

                conn.commit()

            st.session_state["msg_ok"] = "✅ Transferência para Caixa 2 registrada (1 linha em movimentações)."
            st.session_state.form_caixa2 = False
            st.rerun()

        except Exception as e:
            st.error(f"❌ Erro ao transferir: {e}")