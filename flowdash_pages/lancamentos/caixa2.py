import streamlit as st
from .shared import get_conn

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
            with get_conn(caminho_banco) as conn:
                # pega último registro e grava novo saldo simples
                row = conn.execute("SELECT data, caixa, caixa_2 FROM saldos_caixas ORDER BY date(data) DESC LIMIT 1").fetchone()
                caixa_atual  = float(row[1] if row else 0.0)
                caixa2_atual = float(row[2] if row else 0.0)
                novo_caixa  = caixa_atual - float(valor)
                novo_caixa2 = caixa2_atual + float(valor)

                conn.execute("""
                    INSERT INTO saldos_caixas (data, caixa, caixa_2)
                    VALUES (?, ?, ?)
                """, (str(data_lanc), novo_caixa, novo_caixa2))

                # registra no livro de movimentações (opcional)
                conn.execute("""
                    INSERT INTO movimentacoes_bancarias (data, banco, tipo, valor, origem, observacao, referencia_id)
                    VALUES (?, 'Caixa', 'saida', ?, 'transferencia', 'Transferência p/ Caixa 2', NULL)
                """, (str(data_lanc), float(valor)))
                conn.execute("""
                    INSERT INTO movimentacoes_bancarias (data, banco, tipo, valor, origem, observacao, referencia_id)
                    VALUES (?, 'Caixa 2', 'entrada', ?, 'transferencia', 'Transferência de Caixa', NULL)
                """, (str(data_lanc), float(valor)))

                conn.commit()

            st.session_state["msg_ok"] = "✅ Transferência realizada com sucesso!"
            st.session_state.form_caixa2 = False
            st.rerun()

        except Exception as e:
            st.error(f"❌ Erro ao transferir: {e}")