import streamlit as st
from .shared import get_conn
import uuid

def render_deposito(caminho_banco: str, data_lanc):
    if st.button("🏦 Depósito Bancário", use_container_width=True, key="btn_deposito_toggle"):
        st.session_state.form_deposito = not st.session_state.get("form_deposito", False)
    if not st.session_state.get("form_deposito", False):
        return

    # Carregar bancos cadastrados
    nomes_bancos = []
    try:
        with get_conn(caminho_banco) as conn:
            cur = conn.cursor()
            cur.execute("SELECT nome FROM bancos_cadastrados ORDER BY id;")
            nomes_bancos = [row[0] for row in cur.fetchall()]
    except Exception as e:
        st.error(f"Erro ao carregar bancos: {e}")
        return

    st.markdown("#### 🏦 Registrar Depósito Bancário")
    valor = st.number_input("Valor Depositado", min_value=0.0, step=0.01, key="deposito_valor")
    banco = st.selectbox("Banco Destino", nomes_bancos or ["— nenhum banco cadastrado —"], key="deposito_banco")
    confirmar = st.checkbox("Confirmo o depósito", key="deposito_confirmar")

    if st.button("💾 Salvar Depósito", use_container_width=True, key="deposito_salvar"):
        if valor <= 0:
            st.warning("⚠️ Valor inválido.")
            return
        if not nomes_bancos or banco not in nomes_bancos:
            st.warning("⚠️ Selecione um banco válido.")
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

                # 1) Carregar último snapshot de saldos_caixas
                row = cur.execute("""
                    SELECT data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
                    FROM saldos_caixas
                    ORDER BY date(data) DESC, id DESC
                    LIMIT 1
                """).fetchone()

                caixa        = float(row[1]) if row else 0.0
                caixa_2      = float(row[2]) if row else 0.0   # fixo cadastrado
                caixa_vendas = float(row[3]) if row else 0.0
                caixa2_dia   = float(row[5]) if row else 0.0   # variável do dia

                # 2) Validar se há saldo suficiente em caixa2_dia
                if valor_f > caixa2_dia:
                    st.warning(f"⚠️ Saldo insuficiente no Caixa 2 (dia). Disponível: R$ {caixa2_dia:,.2f}.")
                    return

                # 3) Atualizar saldos: subtrai de caixa2_dia
                novo_caixa2_dia   = caixa2_dia - valor_f
                novo_caixa2_total = caixa_2 + novo_caixa2_dia  # regra fixa
                novo_caixa_total  = caixa + caixa_vendas       # regra fixa

                # 4) Inserir novo snapshot em saldos_caixas
                cur.execute("""
                    INSERT INTO saldos_caixas (data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    data_str,
                    caixa,              # inalterado
                    caixa_2,            # cadastrado
                    caixa_vendas,       # inalterado
                    novo_caixa_total,   # recalculado
                    novo_caixa2_dia,    # atualizado
                    novo_caixa2_total   # recalculado
                ))

                # 5) Atualizar saldos_bancos: credita no banco selecionado
                cur.execute("PRAGMA table_info(saldos_bancos);")
                bank_cols = [c[1] for c in cur.fetchall() if c[1] != "data"]

                # garantir que coluna do banco existe
                if banco not in bank_cols:
                    cur.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco}" REAL DEFAULT 0.0;')
                    bank_cols.append(banco)

                # pegar último snapshot de saldos_bancos
                last_bank = cur.execute("""
                    SELECT * FROM saldos_bancos
                    ORDER BY date(data) DESC
                    LIMIT 1
                """).fetchone()

                atuais = {name: 0.0 for name in bank_cols}
                if last_bank:
                    for idx, name in enumerate(["data"] + bank_cols):
                        if name == "data":
                            continue
                        atuais[name] = float(last_bank[idx]) if last_bank[idx] is not None else 0.0

                atuais[banco] = atuais.get(banco, 0.0) + valor_f

                # inserir novo snapshot em saldos_bancos
                placeholders = ", ".join(["?"] * (1 + len(bank_cols)))
                colnames = ", ".join(["data"] + [f'"{c}"' for c in bank_cols])
                values = [data_str] + [atuais[c] for c in bank_cols]
                cur.execute(f'INSERT INTO saldos_bancos ({colnames}) VALUES ({placeholders})', values)

                # 6) Registrar movimentação bancária (uma única linha)
                observ = f"Depósito bancário (origem: Caixa 2 dia)."
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_id, referencia_tabela, trans_uid)
                    VALUES (?, ?, 'entrada', ?, 'deposito_bancario', ?, NULL, NULL, ?)
                """, (data_str, banco, valor_f, observ, trans_uid))

                conn.commit()

            st.session_state["msg_ok"] = "✅ Depósito bancário registrado (Caixa 2 dia → Banco)."
            st.session_state.form_deposito = False
            st.rerun()

        except Exception as e:
            st.error(f"Erro ao salvar depósito: {e}")