import uuid
import streamlit as st
from shared.db import get_conn
from utils.utils import formatar_valor  # para mensagens BRL legíveis


def _r2(x) -> float:
    """Arredonda em 2 casas para evitar ruídos (ex.: -0,00)."""
    return round(float(x or 0.0), 2)


def render_caixa2(caminho_banco: str, data_lanc):
    """
    Transfere dinheiro do 'Caixa'/'Caixa Vendas' para o 'Caixa 2',
    gerando um novo snapshot em saldos_caixas e um único lançamento
    de entrada em movimentacoes_bancarias com origem=transferencia_caixa.
    """
    # Toggle do formulário
    if st.button("🔄 Caixa 2", use_container_width=True, key="btn_caixa2_toggle"):
        st.session_state.form_caixa2 = not st.session_state.get("form_caixa2", False)
    if not st.session_state.get("form_caixa2", False):
        return

    st.markdown("#### 💸 Transferência para Caixa 2")

    # Input de valor
    valor = st.number_input("Valor a Transferir", min_value=0.0, step=0.01, key="caixa2_valor", format="%.2f")

    # Confirmação obrigatória
    confirmar = st.checkbox("Confirmo a transferência", key="caixa2_confirma")

    # Botão desabilitado até marcar a confirmação
    salvar_btn = st.button("💾 Confirmar Transferência", use_container_width=True, key="caixa2_salvar", disabled=not confirmar)

    if salvar_btn:
        if valor <= 0:
            st.warning("⚠️ Valor inválido.")
            return

        try:
            data_str = str(data_lanc)
            trans_uid = str(uuid.uuid4())  # precisa ser único pela restrição UNIQUE
            valor_f = _r2(valor)

            with get_conn(caminho_banco) as conn:
                cur = conn.cursor()

                # 1) Buscar último snapshot
                row = cur.execute("""
                    SELECT data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
                      FROM saldos_caixas
                  ORDER BY date(data) DESC, rowid DESC
                     LIMIT 1
                """).fetchone()

                caixa        = _r2(float(row[1])) if row else 0.0
                caixa_2      = _r2(float(row[2])) if row else 0.0
                caixa_vendas = _r2(float(row[3])) if row else 0.0
                caixa2_dia   = _r2(float(row[5])) if row else 0.0

                caixa_total_atual = _r2(caixa + caixa_vendas)

                # 2) Validação: não transferir mais que o total disponível em dinheiro
                if valor_f > caixa_total_atual:
                    st.warning(f"⚠️ Valor indisponível. Caixa Total atual é {formatar_valor(caixa_total_atual)}.")
                    return

                # 3) Regra: abate primeiro de 'caixa', depois de 'caixa_vendas'
                usar_de_caixa  = _r2(min(valor_f, caixa))
                usar_de_vendas = _r2(valor_f - usar_de_caixa)

                novo_caixa         = _r2(caixa - usar_de_caixa)
                novo_caixa_vendas  = _r2(caixa_vendas - usar_de_vendas)
                novo_caixa2_dia    = _r2(caixa2_dia + valor_f)

                # clamps contra negativos por ruído
                novo_caixa        = max(0.0, novo_caixa)
                novo_caixa_vendas = max(0.0, novo_caixa_vendas)
                novo_caixa2_dia   = max(0.0, novo_caixa2_dia)

                # 4) Recalcular totais
                novo_caixa_total  = _r2(novo_caixa + novo_caixa_vendas)
                novo_caixa2_total = _r2(caixa_2 + novo_caixa2_dia)

                # 5) Gravar snapshot em saldos_caixas
                cur.execute("""
                    INSERT INTO saldos_caixas
                        (data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total)
                    VALUES (?,    ?,     ?,       ?,            ?,            ?,         ?)
                """, (
                    data_str,
                    novo_caixa,          # atualizado
                    caixa_2,             # inalterado
                    novo_caixa_vendas,   # atualizado
                    novo_caixa_total,    # recalculado
                    novo_caixa2_dia,     # atualizado
                    novo_caixa2_total    # recalculado
                ))

                # 6) UMA ÚNICA linha no 'livro': entrada em Caixa 2
                observ = (
                    "Transferência recebida de dinheiro físico | "
                    f"abatido: Caixa={usar_de_caixa:.2f}, Caixa Vendas={usar_de_vendas:.2f}"
                )
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco,   tipo,     valor,  origem,               observacao, referencia_id, referencia_tabela, trans_uid)
                    VALUES (?,   ?,      ?,        ?,      ?,                    ?,          ?,             ?,                 ?)
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