import streamlit as st
import pandas as pd
from datetime import date
from utils.utils import formatar_valor
from .cadastro_classes import CorrecaoCaixaRepository
from repository.movimentacoes_repository import MovimentacoesRepository
from shared.ids import uid_correcao_caixa
from shared.db import get_conn


# ------------------------------------------------------------------------------------
# Lança em movimentacoes_bancarias com referência ao ajuste (com idempotência)
def inserir_mov_bancaria_correcao(
    caminho_banco: str,
    data_: str,
    banco: str,
    valor: float,
    ref_id: int,
    obs: str = ""
) -> int | None:
    """
    Cria um movimento em movimentacoes_bancarias vinculado ao ajuste de caixa,
    preenchendo referencia_tabela, referencia_id e trans_uid (hash).
    Usa registrar_entrada/registrar_saida do MovimentacoesRepository,
    que já é idempotente via trans_uid.
    """
    # normalizações
    data_s = str(data_).strip()
    banco_s = (banco or "").strip()
    obs_s = (obs or "Ajuste manual de caixa").strip()
    ref_tab = "correcao_caixa"
    ref_id_i = int(ref_id) if ref_id else None

    if not banco_s or valor is None or float(valor) == 0.0:
        return None

    # UID padronizado para idempotência
    trans_uid = uid_correcao_caixa(data_s, banco_s, float(valor), obs_s, ref_id_i)

    mov_repo = MovimentacoesRepository(caminho_banco)
    if float(valor) > 0:
        return mov_repo.registrar_entrada(
            data=data_s,
            banco=banco_s,
            valor=float(valor),
            origem="correcao_caixa",
            observacao=obs_s,
            referencia_tabela=ref_tab,
            referencia_id=ref_id_i,
            trans_uid=trans_uid
        )
    else:
        return mov_repo.registrar_saida(
            data=data_s,
            banco=banco_s,
            valor=abs(float(valor)),
            origem="correcao_caixa",
            observacao=obs_s,
            referencia_tabela=ref_tab,
            referencia_id=ref_id_i,
            trans_uid=trans_uid
        )


# ------------------------------------------------------------------------------------
# Opções de conta/banco para correção (Caixa, Caixa 2 + bancos cadastrados)
def carregar_opcoes_banco(caminho_banco: str) -> list[str]:
    opcoes = ["Caixa", "Caixa 2"]
    try:
        with get_conn(caminho_banco) as conn:
            df = pd.read_sql("SELECT nome FROM bancos_cadastrados ORDER BY nome", conn)
        if not df.empty:
            opcoes.extend(df["nome"].tolist())
    except Exception:
        pass
    return opcoes


# ------------------------------------------------------------------------------------
def pagina_correcao_caixa(caminho_banco: str):
    st.subheader("🧮 Correção Manual de Caixa")
    repo = CorrecaoCaixaRepository(caminho_banco)

    # Mensagem pós-rerun
    if st.session_state.get("correcao_msg_ok"):
        st.success(st.session_state.pop("correcao_msg_ok"))

    # Formulário
    data_corrigir = st.date_input("Data do ajuste", value=date.today())
    bancos = carregar_opcoes_banco(caminho_banco)
    destino_banco = st.selectbox("Conta/Banco do ajuste", bancos)

    col1, col2 = st.columns(2)
    with col1:
        # valor pode ser positivo (entrada) ou negativo (saída)
        valor_ajuste = st.number_input(
            "Valor do ajuste (use negativo para saída)",
            step=10.0, format="%.2f"
        )
    with col2:
        lancar_mov = st.checkbox("Lançar também em movimentações bancárias", value=True)

    observacao = st.text_input("Observação (opcional)", value="Ajuste manual de caixa")

    if st.button("✔️ Registrar Ajuste", use_container_width=True):
        try:
            if not destino_banco:
                st.warning("Selecione uma conta/banco.")
                return
            if valor_ajuste == 0:
                st.warning("Informe um valor diferente de zero.")
                return

            # 1) grava ajuste e captura o ID
            ajuste_id = repo.salvar_ajuste(
                data_=str(data_corrigir),
                valor=float(valor_ajuste),
                observacao=observacao or ""
            )

            # 2) opcional: espelhar em movimentações com referência e trans_uid
            mov_id = None
            if lancar_mov:
                mov_id = inserir_mov_bancaria_correcao(
                    caminho_banco=caminho_banco,
                    data_=str(data_corrigir),
                    banco=destino_banco,
                    valor=float(valor_ajuste),
                    ref_id=ajuste_id,
                    obs=observacao
                )

            tipo_txt = "entrada" if valor_ajuste > 0 else "saída"
            msg = (
                f"✅ Ajuste registrado: **{tipo_txt.upper()}** de {formatar_valor(abs(valor_ajuste))} "
                f"em **{destino_banco}** (ID ajuste: {ajuste_id}"
                + (f", mov: {mov_id}" if mov_id else "") + ")."
            )
            st.session_state["correcao_msg_ok"] = msg
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao registrar ajuste: {e}")

    st.markdown("---")

    # Resumo do dia escolhido
    st.markdown("### 📅 Resumo do dia selecionado")
    try:
        df_aj = repo.listar_ajustes()
        if isinstance(df_aj, pd.DataFrame) and not df_aj.empty:
            col_data = "data" if "data" in df_aj.columns else None
            col_valor = "valor" if "valor" in df_aj.columns else None

            if col_data:
                df_aj[col_data] = pd.to_datetime(df_aj[col_data], errors="coerce")
                df_dia = df_aj[df_aj[col_data].dt.date == data_corrigir]
            else:
                df_dia = pd.DataFrame()

            if not df_dia.empty and col_valor:
                total_pos = df_dia[df_dia[col_valor] > 0][col_valor].sum()
                total_neg = df_dia[df_dia[col_valor] < 0][col_valor].sum()
                st.info(
                    f"**Entradas:** {formatar_valor(total_pos)} • "
                    f"**Saídas:** {formatar_valor(abs(total_neg))} • "
                    f"**Saldo do dia:** {formatar_valor((total_pos + total_neg))}"
                )
            else:
                st.caption("Sem ajustes para esta data.")
        else:
            st.caption("Sem ajustes cadastrados.")
    except Exception as e:
        st.error(f"Erro ao verificar correções do dia: {e}")

    # Histórico geral
    st.markdown("### 🗂️ Histórico de ajustes")
    try:
        df_ajustes = repo.listar_ajustes()
        if isinstance(df_ajustes, pd.DataFrame) and not df_ajustes.empty:
            if "data" in df_ajustes.columns:
                df_ajustes["data"] = pd.to_datetime(df_ajustes["data"]).dt.strftime("%d/%m/%Y")
            if "valor" in df_ajustes.columns:
                df_ajustes["valor"] = df_ajustes["valor"].apply(formatar_valor)

            ren = {}
            if "data" in df_ajustes.columns:
                ren["data"] = "Data"
            if "valor" in df_ajustes.columns:
                ren["valor"] = "Valor (R$)"
            if "observacao" in df_ajustes.columns:
                ren["observacao"] = "Observação"

            df_show = df_ajustes.rename(columns=ren)
            cols = [c for c in ["Data", "Valor (R$)", "Observação"] if c in df_show.columns]
            st.dataframe(df_show[cols], use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum ajuste registrado ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar ajustes: {e}")