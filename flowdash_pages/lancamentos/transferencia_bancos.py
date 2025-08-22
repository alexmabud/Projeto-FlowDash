"""
Módulo TransferenciaBancos (Lançamentos)
========================================

Este módulo define a página e a lógica para registrar **transferências entre bancos** 
ou entre contas diferentes dentro do sistema. Ele permite movimentar saldos de forma 
controlada e idempotente, sem gerar distorções no fluxo de entradas e saídas.

Funcionalidades principais
--------------------------
- Registro de transferências entre contas bancárias distintas.
- Ajuste automático dos saldos nas tabelas correspondentes.
- Registro de movimentações no histórico (`movimentacoes_bancarias`).
- Prevenção de duplicidade via integração com `LedgerService`.
- Interface em Streamlit para escolha do banco de origem e destino.

Detalhes técnicos
-----------------
- Implementado em Streamlit para interação via formulário.
- Validações garantem que o banco de origem e o de destino não sejam iguais.
- A operação é neutra no fluxo de caixa (não afeta entradas/saídas globais),
  mas altera os saldos individuais de cada banco.
- Cada transferência gera dois registros:
  - Saída no banco de origem.
  - Entrada no banco de destino.

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


def _date_col_name(conn, table: str) -> str:
    """Descobre o nome da coluna de data ('data' ou 'Data')."""
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]
    for cand in ("data", "Data"):
        if cand in cols:
            return cand
    return "data"


def _subtrair_saldo_banco(caminho_banco: str, data_str: str, banco_nome: str, valor: float) -> None:
    """
    Subtrai 'valor' do saldo do banco na linha da 'data_str' em saldos_bancos.
    - Garante a coluna do banco.
    - Se a data não existir, cria a linha e subtrai nela.
    (Não usa upsert_saldos_bancos pois ela rejeita valores <= 0.)
    """
    if not valor or valor <= 0:
        return

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # Garante que o banco está cadastrado
        try:
            nomes_cadastrados = pd.read_sql("SELECT nome FROM bancos_cadastrados", conn)["nome"].astype(str).tolist()
        except Exception:
            nomes_cadastrados = []
        if banco_nome not in nomes_cadastrados:
            raise ValueError(f"Banco '{banco_nome}' não está cadastrado em bancos_cadastrados.")

        # Garante coluna do banco
        cols_info = cur.execute("PRAGMA table_info(saldos_bancos);").fetchall()
        existentes = {c[1] for c in cols_info}
        if banco_nome not in existentes:
            cur.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_nome}" REAL NOT NULL DEFAULT 0.0')
            conn.commit()
            cols_info = cur.execute("PRAGMA table_info(saldos_bancos);").fetchall()

        date_col = _date_col_name(conn, "saldos_bancos")

        # Se existe linha da data → UPDATE; senão → INSERT
        row = cur.execute(f'SELECT rowid FROM saldos_bancos WHERE "{date_col}"=? LIMIT 1;', (data_str,)).fetchone()
        if row:
            cur.execute(
                f'UPDATE saldos_bancos '
                f'SET "{banco_nome}" = COALESCE("{banco_nome}", 0.0) - ? '
                f'WHERE "{date_col}" = ?;',
                (float(valor), data_str)
            )
        else:
            # Monta um INSERT com -valor para o banco escolhido
            colnames = [c[1] for c in cols_info]  # inclui a coluna de data e as demais
            outras = [c for c in colnames if c != date_col]
            placeholders = ",".join(["?"] * (1 + len(outras)))
            cols_sql = f'"{date_col}",' + ",".join(f'"{c}"' for c in outras)
            valores = [data_str] + [0.0] * len(outras)
            if banco_nome in outras:
                valores[1 + outras.index(banco_nome)] = -float(valor)
            else:
                raise RuntimeError(f"Coluna '{banco_nome}' não encontrada após criação em saldos_bancos.")
            cur.execute(f'INSERT INTO saldos_bancos ({cols_sql}) VALUES ({placeholders});', valores)

        conn.commit()


def render_transferencia_bancaria(caminho_banco: str, data_lanc):
    """
    Transfere saldo entre dois bancos (banco → banco).

    Efeitos:
      - movimentacoes_bancarias: cria 2 linhas
          * SAÍDA no banco de origem (origem='transf_bancos_saida')
          * ENTRADA no banco de destino (origem='transf_bancos')
        (o card de resumo soma apenas 'origem = transf_bancos', então contará só a ENTRADA)
      - saldos_bancos: subtrai no banco de origem e soma no destino (na mesma data)

    As DUAS linhas recebem o MESMO `referencia_id` (id da SAÍDA).
    `trans_uid` é único por linha (UNIQUE no schema).
    Observações automáticas incluem o valor e REF.
    """

    # Toggle do formulário
    if st.button("🔁 Transferência entre Bancos", use_container_width=True, key="btn_transf_bancos_toggle"):
        st.session_state.form_transf_bancos = not st.session_state.get("form_transf_bancos", False)
        if st.session_state.form_transf_bancos:
            st.session_state["transf_bancos_confirmar"] = False

    if not st.session_state.get("form_transf_bancos", False):
        return

    st.markdown("#### 🔁 Transferência Banco → Banco")
    st.caption(f"Data do lançamento: **{pd.to_datetime(data_lanc).strftime('%d/%m/%Y')}**")

    # Bancos cadastrados
    bancos_repo = BancoRepository(caminho_banco)
    df_bancos = bancos_repo.carregar_bancos()
    nomes_bancos = df_bancos["nome"].tolist() if not df_bancos.empty else []

    col_a, col_b = st.columns(2)
    with col_a:
        banco_origem = (
            st.selectbox("Banco de Origem", nomes_bancos, key="transf_banco_origem")
            if nomes_bancos else
            st.text_input("Banco de Origem (digite)", key="transf_banco_origem_text")
        )
    with col_b:
        banco_destino = (
            st.selectbox("Banco de Destino", nomes_bancos, key="transf_banco_destino")
            if nomes_bancos else
            st.text_input("Banco de Destino (digite)", key="transf_banco_destino_text")
        )

    valor = st.number_input("Valor da Transferência", min_value=0.0, step=0.01, format="%.2f", key="transf_bancos_valor")

    st.info("\n".join([
        "**Confirme os dados da transferência**",
        f"- **Data:** {pd.to_datetime(data_lanc).strftime('%d/%m/%Y')}",
        f"- **Valor:** {formatar_valor(valor or 0.0)}",
        f"- **Origem:** {(banco_origem or '—')}",
        f"- **Destino:** {(banco_destino or '—')}",
        "- Serão criadas 2 linhas com o MESMO referencia_id (id da SAÍDA).",
    ]))

    confirmar = st.checkbox("Confirmo os dados acima", key="transf_bancos_confirmar")
    salvar_btn = st.button("💾 Registrar Transferência", use_container_width=True, key="transf_bancos_salvar", disabled=not confirmar)
    if not salvar_btn:
        return
    if not st.session_state.get("transf_bancos_confirmar", False):
        st.warning("⚠️ Confirme os dados antes de salvar.")
        return

    # Validações
    valor_f = _r2(valor)
    if valor_f <= 0:
        st.warning("⚠️ Valor inválido.")
        return

    b_origem_in = (banco_origem or "").strip()
    b_dest_in   = (banco_destino or "").strip()
    if not b_origem_in or not b_dest_in:
        st.warning("⚠️ Informe banco de origem e banco de destino.")
        return
    if b_origem_in.lower() == b_dest_in.lower():
        st.warning("⚠️ Origem e destino não podem ser o mesmo banco.")
        return

    # Canonicalizar nomes
    try:
        b_origem = canonicalizar_banco(caminho_banco, b_origem_in) or b_origem_in
    except Exception:
        b_origem = b_origem_in
    try:
        b_dest = canonicalizar_banco(caminho_banco, b_dest_in) or b_dest_in
    except Exception:
        b_dest = b_dest_in

    data_str = str(data_lanc)

    try:
        with get_conn(caminho_banco) as conn:
            cur = conn.cursor()

            # 1) SAÍDA no banco de origem (inserimos primeiro para obter o id da SAÍDA)
            saida_uid = str(uuid.uuid4())  # trans_uid único por linha (UNIQUE no schema)
            saida_obs = (
                f"Transferência: {b_origem} → {b_dest} | Saída | "
                f"Valor={formatar_valor(valor_f)}"
            )
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco,  tipo,   valor,  origem,                observacao,
                     referencia_id, referencia_tabela, trans_uid)
                VALUES (?,   ?,     ?,      ?,      ?,                     ?,
                        ?,             ?,                 ?)
            """, (
                data_str, b_origem, "saida", valor_f, "transf_bancos_saida",
                saida_obs,
                None, "movimentacoes_bancarias", saida_uid
            ))
            saida_id = cur.lastrowid

            # 2) ENTRADA no banco de destino (MESMO referencia_id = id da SAÍDA)
            entrada_uid = str(uuid.uuid4())  # outro trans_uid único
            entrada_obs = (
                f"Transferência: {b_origem} → {b_dest} | Entrada | "
                f"Valor={formatar_valor(valor_f)} | REF={saida_id}"
            )
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco,   tipo,     valor,  origem,         observacao,
                     referencia_id, referencia_tabela, trans_uid)
                VALUES (?,   ?,      ?,        ?,      ?,              ?,
                        ?,             ?,                 ?)
            """, (
                data_str, b_dest, "entrada", valor_f, "transf_bancos",
                entrada_obs,
                saida_id, "movimentacoes_bancarias", entrada_uid
            ))
            entrada_id = cur.lastrowid

            # 3) Atualiza a SAÍDA para também ter referencia_id = id da SAÍDA e REF na observação
            saida_obs_final = (
                f"Transferência: {b_origem} → {b_dest} | Saída | "
                f"Valor={formatar_valor(valor_f)} | REF={saida_id}"
            )
            cur.execute("""
                UPDATE movimentacoes_bancarias
                   SET referencia_id = ?,
                       referencia_tabela = ?,
                       observacao = ?
                 WHERE id = ?
            """, (saida_id, "movimentacoes_bancarias", saida_obs_final, saida_id))

            conn.commit()

        # 4) Atualiza saldos_bancos (mesma data)
        #    - soma no DESTINO
        try:
            upsert_saldos_bancos(caminho_banco, data_str, b_dest, valor_f)
        except Exception as e:
            st.warning(f"Não foi possível somar no destino '{b_dest}' em saldos_bancos: {e}")

        #    - subtrai na ORIGEM
        try:
            _subtrair_saldo_banco(caminho_banco, data_str, b_origem, valor_f)
        except Exception as e:
            st.warning(f"Não foi possível subtrair na origem '{b_origem}' em saldos_bancos: {e}")

        st.session_state["msg_ok"] = (
            f"✅ Transferência registrada: {formatar_valor(valor_f)} "
            f"de **{b_origem}** → **{b_dest}**. "
            f"(IDs: saída #{saida_id}, entrada #{entrada_id} • ref_id comum={saida_id})"
        )
        st.session_state.form_transf_bancos = False
        st.rerun()

    except Exception as e:
        st.error(f"❌ Erro ao registrar transferência: {e}")