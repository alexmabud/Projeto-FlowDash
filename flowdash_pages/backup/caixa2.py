"""
Módulo Caixa (Lançamentos)
==========================

Este módulo define a página e a lógica para registrar e controlar o **caixa físico**
da loja no sistema FlowDash. Ele é responsável por acompanhar o dinheiro em espécie
que entra e sai, garantindo consistência entre **Caixa** e **Caixa 2**.

Funcionalidades principais
--------------------------
- Registro de movimentações em dinheiro (entradas e saídas manuais).
- Controle separado entre:
  - **Caixa** → dinheiro que permanece na loja.
  - **Caixa 2** → dinheiro retirado da loja e levado para casa/depósito futuro.
- Suporte a transferências entre Caixa → Caixa 2 (com registro automático).
- Integração com `LedgerService` para garantir idempotência e consistência.
- Exibição em Streamlit com formulário dinâmico e totais consolidados.

Detalhes técnicos
-----------------
- Implementado em Streamlit.
- Ajusta os saldos em `saldos_caixas` e gera movimentações correspondentes
  em `movimentacoes_bancarias`.
- Permite consultas por período para auditoria.
- Integra-se ao fechamento de caixa e relatórios financeiros (DRE).

Dependências
------------
- streamlit
- pandas
- datetime
- services.ledger.LedgerService
- repository.movimentacoes_repository.MovimentacoesRepository
- shared.db.get_conn

"""

import uuid
import streamlit as st
from shared.db import get_conn
from utils.utils import formatar_valor

def _r2(x) -> float:
    return round(float(x or 0.0), 2)

def render_caixa2(caminho_banco: str, data_lanc):
    # Toggle do formulário
    if st.button("🔄 Caixa 2", use_container_width=True, key="btn_caixa2_toggle"):
        st.session_state.form_caixa2 = not st.session_state.get("form_caixa2", False)
    if not st.session_state.get("form_caixa2", False):
        return

    st.markdown("#### 💸 Transferência para Caixa 2")

    valor = st.number_input("Valor a Transferir", min_value=0.0, step=0.01,
                            key="caixa2_valor", format="%.2f")
    confirmar = st.checkbox("Confirmo a transferência", key="caixa2_confirma")
    salvar_btn = st.button("💾 Confirmar Transferência", use_container_width=True,
                           key="caixa2_salvar", disabled=not confirmar)
    if not salvar_btn:
        return
    if valor <= 0:
        st.warning("⚠️ Valor inválido.")
        return

    try:
        data_str = str(data_lanc)  # ISO YYYY-MM-DD (facilita filtros)
        valor_f = _r2(valor)

        with get_conn(caminho_banco) as conn:
            cur = conn.cursor()

            # 1) Snapshot do DIA (se não houver, baseia no último < data)
            same = cur.execute("""
                SELECT id, data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
                  FROM saldos_caixas
                 WHERE date(data)=?
              ORDER BY id DESC
                 LIMIT 1
            """, (data_str,)).fetchone()

            if same:
                snap_id        = same[0]
                base_caixa     = _r2(same[2])
                base_caixa2    = _r2(same[3])   # coluna: caixa_2 (saldo acumulado do Caixa 2)
                base_vendas    = _r2(same[4])
                base_caixa2dia = _r2(same[6])   # valor movimentado no dia para Caixa 2
            else:
                prev = cur.execute("""
                    SELECT data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
                      FROM saldos_caixas
                     WHERE date(data) < ?
                  ORDER BY date(data) DESC, id DESC
                     LIMIT 1
                """, (data_str,)).fetchone()
                snap_id        = None
                base_caixa     = _r2(prev[1]) if prev else 0.0
                base_caixa2    = _r2(prev[2]) if prev else 0.0
                base_vendas    = _r2(prev[3]) if prev else 0.0
                base_caixa2dia = 0.0  # novo dia começa em 0

            base_total_dinheiro = _r2(base_caixa + base_vendas)
            if valor_f > base_total_dinheiro:
                st.warning(f"⚠️ Valor indisponível. Caixa Total atual é {formatar_valor(base_total_dinheiro)}.")
                return

            # 2) Abate primeiro de 'caixa', depois de 'caixa_vendas'
            usar_caixa  = _r2(min(valor_f, base_caixa))
            usar_vendas = _r2(valor_f - usar_caixa)

            novo_caixa       = max(0.0, _r2(base_caixa - usar_caixa))
            novo_vendas      = max(0.0, _r2(base_vendas - usar_vendas))
            novo_caixa2_dia  = max(0.0, _r2(base_caixa2dia + valor_f))
            novo_caixa_total = _r2(novo_caixa + novo_vendas)
            novo_caixa2_tot  = _r2(base_caixa2 + novo_caixa2_dia)  # caixa_2 é a base, dia é o movimento

            # 3) UPSERT no snapshot do dia (uma linha por dia)
            if snap_id is not None:
                cur.execute("""
                    UPDATE saldos_caixas
                       SET caixa=?,
                           caixa_vendas=?,
                           caixa_total=?,
                           caixa2_dia=?,
                           caixa2_total=?
                     WHERE id=?
                """, (novo_caixa, novo_vendas, novo_caixa_total,
                      novo_caixa2_dia, novo_caixa2_tot, snap_id))
            else:
                cur.execute("""
                    INSERT INTO saldos_caixas
                        (data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total)
                    VALUES (?,    ?,     ?,       ?,            ?,           ?,          ?)
                """, (data_str, novo_caixa, base_caixa2, novo_vendas,
                      novo_caixa_total, novo_caixa2_dia, novo_caixa2_tot))

            # 4) LIVRO: **uma linha por lançamento** (sem agregar no dia)
            trans_uid = str(uuid.uuid4())  # UNIQUE por linha
            observ = (
                f"Transferência p/ Caixa 2 | "
                f"Valor={formatar_valor(valor_f)} | "
                f"C={usar_caixa:.2f}; V={usar_vendas:.2f}"
            )
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco,   tipo,     valor,  origem,               observacao,
                     referencia_id, referencia_tabela, trans_uid)
                VALUES (?,   ?,      ?,        ?,      ?,                    ?,
                        ?,             ?,                 ?)
            """, (
                data_str, "Caixa 2", "entrada", valor_f,
                "transferencia_caixa", observ,
                None, "movimentacoes_bancarias", trans_uid
            ))
            mov_id = cur.lastrowid

            # Self-reference: mesma linha recebe referencia_id = seu id e REF na observação
            observ_final = observ + f" | REF={mov_id}"
            cur.execute("""
                UPDATE movimentacoes_bancarias
                   SET referencia_id = ?, observacao = ?
                 WHERE id = ?
            """, (mov_id, observ_final, mov_id))

            conn.commit()

        st.session_state["msg_ok"] = (
            f"✅ Transferência para Caixa 2 registrada: {formatar_valor(valor_f)} "
            f"(abatido — Caixa: {formatar_valor(usar_caixa)}, Vendas: {formatar_valor(usar_vendas)})."
        )
        st.session_state.form_caixa2 = False
        st.rerun()

    except Exception as e:
        st.error(f"❌ Erro ao transferir: {e}")