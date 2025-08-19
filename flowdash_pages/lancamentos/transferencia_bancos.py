import streamlit as st
from .shared import get_conn
import uuid

def render_transferencia_bancaria(caminho_banco: str, data_lanc):
    # Toggle padrão (compatível com sua página que seta st.session_state.form_transf_bancos)
    if st.button("🔁 Transferência entre Bancos", use_container_width=True, key="btn_transf_bancos_toggle"):
        st.session_state.form_transf_bancos = not st.session_state.get("form_transf_bancos", False)
    if not st.session_state.get("form_transf_bancos", False):
        return

    # Carrega bancos cadastrados
    nomes_bancos = []
    try:
        with get_conn(caminho_banco) as conn:
            cur = conn.cursor()
            cur.execute("SELECT nome FROM bancos_cadastrados ORDER BY id;")
            nomes_bancos = [r[0] for r in cur.fetchall()]
    except Exception as e:
        st.error(f"Erro ao carregar bancos: {e}")
        return

    st.markdown("#### 🔁 Transferência entre Bancos")
    col1, col2 = st.columns(2)
    with col1:
        banco_origem = st.selectbox("Banco de Origem", nomes_bancos or ["— nenhum —"], key="transf_banco_origem")
    with col2:
        banco_destino = st.selectbox("Banco de Destino", nomes_bancos or ["— nenhum —"], key="transf_banco_destino")

    valor = st.number_input("Valor da Transferência", min_value=0.0, step=0.01, key="transf_bancos_valor")
    confirmar = st.checkbox("Confirmo a transferência", key="transf_bancos_confirmar")

    if st.button("💾 Confirmar Transferência", use_container_width=True, key="transf_bancos_salvar"):
        if valor <= 0:
            st.warning("⚠️ Valor inválido.")
            return
        if not nomes_bancos or banco_origem not in nomes_bancos or banco_destino not in nomes_bancos:
            st.warning("⚠️ Selecione bancos válidos.")
            return
        if banco_origem == banco_destino:
            st.warning("⚠️ Origem e destino devem ser diferentes.")
            return
        if not confirmar:
            st.warning("⚠️ Confirme os dados antes de salvar.")
            return

        try:
            data_str = str(data_lanc)
            trans_uid = str(uuid.uuid4())
            valor_f = float(valor)

            with get_conn(caminho_banco) as conn:
                cur = conn.cursor()

                # Garante colunas em saldos_bancos
                cur.execute("PRAGMA table_info(saldos_bancos);")
                bank_cols = [c[1] for c in cur.fetchall() if c[1] != "data"]
                for nome in (banco_origem, banco_destino):
                    if nome not in bank_cols:
                        cur.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{nome}" REAL DEFAULT 0.0;')
                        bank_cols.append(nome)

                # Último snapshot de saldos_bancos
                last_row = cur.execute("""
                    SELECT * FROM saldos_bancos
                    ORDER BY date(data) DESC
                    LIMIT 1
                """).fetchone()

                atuais = {name: 0.0 for name in bank_cols}
                if last_row:
                    cols_in_order = ["data"] + bank_cols
                    for idx, name in enumerate(cols_in_order):
                        if name == "data":
                            continue
                        atuais[name] = float(last_row[idx]) if last_row[idx] is not None else 0.0

                # Valida saldo no banco de origem
                saldo_origem = atuais.get(banco_origem, 0.0)
                if valor_f > saldo_origem:
                    st.warning(f"⚠️ Saldo insuficiente no banco de origem ({banco_origem}). Disponível: R$ {saldo_origem:,.2f}.")
                    return

                # Debita origem / credita destino
                atuais[banco_origem] = saldo_origem - valor_f
                atuais[banco_destino] = atuais.get(banco_destino, 0.0) + valor_f

                # Insere novo snapshot em saldos_bancos
                placeholders = ", ".join(["?"] * (1 + len(bank_cols)))
                colnames = ", ".join(["data"] + [f'"{c}"' for c in bank_cols])
                values = [data_str] + [atuais[c] for c in bank_cols]
                cur.execute(f'INSERT INTO saldos_bancos ({colnames}) VALUES ({placeholders})', values)

                # ÚNICA linha em movimentacoes_bancarias (para não inflar fechamento)
                observ = f"Transferência interna de {banco_origem} → {banco_destino}"
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_id, referencia_tabela, trans_uid)
                    VALUES (?, ?, 'entrada', ?, 'transferencia_bancaria', ?, NULL, NULL, ?)
                """, (data_str, banco_destino, valor_f, observ, trans_uid))

                conn.commit()

            st.session_state["msg_ok"] = "✅ Transferência entre bancos registrada."
            st.session_state.form_transf_bancos = False
            st.rerun()

        except Exception as e:
            st.error(f"❌ Erro na transferência: {e}")