import streamlit as st
from .shared import get_conn

def render_deposito(caminho_banco: str, data_lanc):
    if st.button("🏦 Depósito Bancário", use_container_width=True, key="btn_deposito_toggle"):
        st.session_state.form_deposito = not st.session_state.get("form_deposito", False)
    if not st.session_state.get("form_deposito", False):
        return

    st.markdown("#### 🏦 Registrar Depósito Bancário")
    valor = st.number_input("Valor Depositado", min_value=0.0, step=0.01, key="deposito_valor")
    banco = st.selectbox("Banco Destino", ["Banco 1","Banco 2","Banco 3","Banco 4"], key="deposito_banco")
    confirmar = st.checkbox("Confirmo o depósito", key="deposito_confirmar")

    if st.button("💾 Salvar Depósito", use_container_width=True, key="deposito_salvar"):
        if valor <= 0:
            st.warning("⚠️ Valor inválido.")
            return
        if not confirmar:
            st.warning("⚠️ Confirme os dados antes de salvar.")
            return

        try:
            with get_conn(caminho_banco) as conn:
                conn.execute("""
                    INSERT INTO movimentacoes_bancarias (data, banco, tipo, valor, origem, observacao, referencia_id)
                    VALUES (?, ?, 'entrada', ?, 'deposito', 'Depósito em dinheiro', NULL)
                """, (str(data_lanc), banco, float(valor)))
                conn.commit()
            st.session_state["msg_ok"] = "✅ Depósito registrado!"
            st.session_state.form_deposito = False
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar depósito: {e}")