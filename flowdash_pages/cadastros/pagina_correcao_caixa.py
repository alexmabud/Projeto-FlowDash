import streamlit as st
import sqlite3
import pandas as pd
from datetime import date
from utils.utils import formatar_valor
from .cadastro_classes import CorrecaoCaixaRepository

# Página para correção manual de caixa ========================================================================
def carregar_opcoes_banco(caminho_banco: str) -> list[str]:
    opcoes = ["Caixa", "Caixa 2"]
    try:
        with sqlite3.connect(caminho_banco) as conn:
            df = pd.read_sql("SELECT nome FROM bancos_cadastrados ORDER BY nome", conn)
        if not df.empty:
            opcoes.extend(df["nome"].tolist())
    except Exception:
        pass
    # dedup preservando ordem
    seen, dedup = set(), []
    for x in opcoes:
        if x not in seen:
            dedup.append(x); seen.add(x)
    return dedup

def inserir_mov_bancaria_correcao(
    caminho_banco: str,
    data_: str,
    banco: str,
    valor: float,
    ref_id: int | None,
    obs: str = ""
) -> int | None:
    """Insere linha em movimentacoes_bancarias e retorna o ID criado (ou None)."""
    if not valor:
        return None
    tipo = "entrada" if valor > 0 else "saida"
    with sqlite3.connect(caminho_banco) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO movimentacoes_bancarias (data, banco, tipo, valor, origem, observacao, referencia_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (data_, banco, tipo, abs(valor), "correcao_caixa", obs, ref_id))
        conn.commit()
        return cur.lastrowid

# --------- página ----------
def pagina_correcao_caixa(caminho_banco: str):
    st.subheader("🛠️ Correção Manual de Caixa")
    repo = CorrecaoCaixaRepository(caminho_banco)

    # --- Flash: mostrar tudo que foi feito no último salvar ---
    if "ajuste_flash_msgs" in st.session_state:
        for msg in st.session_state["ajuste_flash_msgs"]:
            st.success(msg)
        # limpa para a próxima interação
        del st.session_state["ajuste_flash_msgs"]

    # formulário (com keys para persistir após rerun)
    data_corrigir = st.date_input("Data do Ajuste", value=date.today(), key="ajuste_data")
    valor_ajuste = st.number_input("Valor de Correção (positivo ou negativo)", step=10.0, format="%.2f", key="ajuste_valor")
    observacao = st.text_input("Motivo ou Observação", max_chars=200, key="ajuste_obs")

    col1, col2 = st.columns([2, 1])
    with col1:
        destino_banco = st.selectbox("Afeta onde?", carregar_opcoes_banco(caminho_banco), key="ajuste_destino")
    with col2:
        lancar_mov = st.checkbox("Lançar em movimentações", value=True, key="ajuste_lancar_mov")

    if st.button("💾 Salvar Ajuste Manual", use_container_width=True):
        try:
            if valor_ajuste == 0:
                st.warning("Informe um valor diferente de zero.")
                st.stop()

            msgs = []

            # 1) salvar ajuste e pegar id
            ajuste_id = repo.salvar_ajuste(str(data_corrigir), valor_ajuste, observacao)
            valor_fmt = f"R$ {abs(valor_ajuste):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            msgs.append(
                f"Correção salva (ID **{ajuste_id}**) em **{pd.to_datetime(data_corrigir).strftime('%d/%m/%Y')}** • "
                f"Valor: **{valor_fmt}** ({'entrada' if valor_ajuste > 0 else 'saída'}) • "
                f"Obs: **{observacao or '—'}**"
            )

            # 2) espelhar em movimentações (opcional)
            if lancar_mov:
                mov_id = inserir_mov_bancaria_correcao(
                    caminho_banco=caminho_banco,
                    data_=str(data_corrigir),
                    banco=destino_banco,
                    valor=valor_ajuste,
                    ref_id=ajuste_id,
                    obs=observacao
                )
                msgs.append(
                    f"Lançamento criado em **movimentacoes_bancarias** (ID **{mov_id}**) • "
                    f"Conta: **{destino_banco}** • Tipo: **{'entrada' if valor_ajuste > 0 else 'saída'}**"
                )

            # 3) guarda todas as mensagens e rerun
            st.session_state["ajuste_flash_msgs"] = msgs
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar correção: {e}")

    # aviso da correção do dia (mantido)
    try:
        df_ajustes = repo.listar_ajustes()
        if not df_ajustes.empty:
            df_ajustes["data"] = pd.to_datetime(df_ajustes["data"], errors="coerce")
            ajustes_data = df_ajustes[df_ajustes["data"].dt.date == st.session_state.get("ajuste_data", date.today())]
            if not ajustes_data.empty:
                # tenta ordenar por id (se existir)
                if "id" in ajustes_data.columns:
                    ajustes_data = ajustes_data.sort_values("id")
                ultimo = ajustes_data.iloc[-1]
                valor_formatado = f"R$ {ultimo['valor']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                obs = ultimo.get("observacao") or "Nenhuma"
                st.info(
                    f"ℹ️ Correção para **{st.session_state['ajuste_data'].strftime('%d/%m/%Y')}**:\n\n"
                    f"- 💰 Valor: {valor_formatado}\n"
                    f"- 📝 Observação: {obs}"
                )
    except Exception as e:
        st.error(f"Erro ao verificar correções do dia: {e}")

    # histórico
    try:
        df_ajustes = repo.listar_ajustes()
        if not df_ajustes.empty:
            df_ajustes["data"] = pd.to_datetime(df_ajustes["data"]).dt.strftime("%d/%m/%Y")
            df_ajustes["valor"] = df_ajustes["valor"].apply(formatar_valor)
            df_ajustes.rename(columns={"data": "Data", "valor": "Valor (R$)", "observacao": "Observação"}, inplace=True)
            st.dataframe(df_ajustes[["Data", "Valor (R$)", "Observação"]], use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum ajuste registrado ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar ajustes: {e}")