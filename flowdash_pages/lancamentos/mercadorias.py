import streamlit as st
from .shared import get_conn

def render_mercadorias(caminho_banco: str, data_lanc):
    if st.button("📦 Mercadorias", use_container_width=True, key="btn_mercadoria_toggle"):
        st.session_state.form_mercadoria = not st.session_state.get("form_mercadoria", False)
    if not st.session_state.get("form_mercadoria", False):
        return

    st.markdown("#### 📦 Registro de Mercadorias")
    fornecedor = st.text_input("Fornecedor", key="merc_forn")
    valor = st.number_input("Valor da Mercadoria", min_value=0.0, step=0.01, key="merc_valor")
    frete = st.number_input("Frete", min_value=0.0, step=0.01, key="merc_frete")
    forma = st.selectbox("Forma de Pagamento", ["DINHEIRO","PIX","DÉBITO","CRÉDITO"], key="merc_forma")

    confirmar = st.checkbox("Confirmo os dados", key="merc_confirma")
    if st.button("💾 Salvar Mercadoria", use_container_width=True, key="merc_salvar"):
        if not fornecedor or valor <= 0:
            st.warning("⚠️ Preencha fornecedor e valor.")
            return
        if not confirmar:
            st.warning("⚠️ Confirme os dados.")
            return
        try:
            with get_conn(caminho_banco) as conn:
                conn.execute("""
                    INSERT INTO mercadorias (Data, Fornecedor, Valor_Mercadoria, Frete, Forma_Pagamento)
                    VALUES (?, ?, ?, ?, ?)
                """, (str(data_lanc), fornecedor, float(valor), float(frete or 0.0), forma))
                conn.commit()
            st.session_state["msg_ok"] = "✅ Mercadoria registrada!"
            st.session_state.form_mercadoria = False
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar mercadoria: {e}")