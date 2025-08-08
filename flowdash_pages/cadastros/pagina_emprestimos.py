import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, datetime
from flowdash_pages.cadastros.cadastro_classes import EmprestimoRepository, BancoRepository
from utils.utils import formatar_valor

TIPOS_EMPRESTIMO = ["Empréstimo", "Financiamento", "Crédito Pessoal", "Outro"]
STATUS_OPCOES = ["Em aberto", "Quitado", "Renegociado"]

# ===== PÁGINA DE EMPRÉSTIMOS E FINANCIAMENTOS =================================================
def pagina_emprestimos_financiamentos(caminho_banco: str):
    st.subheader("🏦 Cadastro de Empréstimos e Financiamentos")

    repo = EmprestimoRepository(caminho_banco)
    banco_repo = BancoRepository(caminho_banco)
    df_bancos = banco_repo.carregar_bancos()
    nomes_bancos = df_bancos["nome"].tolist() if not df_bancos.empty else []

    # Mensagem pós-salvamento
    if st.session_state.get("sucesso_emprestimo"):
        st.success("✅ Empréstimo salvo com sucesso!")
        del st.session_state["sucesso_emprestimo"]

    # ===================== 1) NOVO EMPRÉSTIMO (TOPO) =====================
    with st.expander("➕ Cadastrar novo empréstimo / financiamento", expanded=True):
        with st.form("form_novo_emprestimo"):
            col1, col2, col3 = st.columns(3)
            with col1:
                data_contratacao = st.date_input("Data da contratação", value=date.today())
                valor_total = st.number_input("Valor total (R$)", min_value=0.0, step=10.0, format="%.2f")
                tipo = st.selectbox("Tipo", ["Empréstimo", "Financiamento", "Crédito Pessoal", "Outro"])
            with col2:
                banco = st.selectbox("Banco", nomes_bancos) if nomes_bancos else st.text_input("Banco (digite)")
                parcelas_total = st.number_input("Total de parcelas", min_value=1, max_value=360, step=1, value=12)
                parcelas_pagas = st.number_input("Parcelas pagas", min_value=0, max_value=360, step=1, value=0)
            with col3:
                valor_parcela = st.number_input("Valor da parcela (R$)", min_value=0.0, step=1.0, format="%.2f")
                taxa_juros_am = st.number_input("Juros a.m. (%)", min_value=0.0, step=0.01, format="%.2f")
                vencimento_dia = st.number_input("Dia de vencimento", min_value=1, max_value=31, step=1, value=10)

            col4, col5, col6 = st.columns(3)
            with col4:
                status = st.selectbox("Status", ["Em aberto", "Quitado", "Renegociado"])
            with col5:
                origem_recursos = st.text_input("Origem dos recursos (opcional)")
                renegociado_de = st.text_input("Renegociado de (ID/descrição) (opcional)")
            with col6:
                descricao = st.text_input("Descrição (opcional)")

            # 🔴 Sem checkbox “tem data de quitação”
            # Mostra o calendário somente se status == "Quitado"
            data_quitacao = None
            if status == "Quitado":
                data_quitacao = st.date_input("Data de quitação", value=date.today(), key="data_quitacao_novo")

            # Cálculos auxiliares
            valor_pago = round(parcelas_pagas * valor_parcela, 2)
            valor_em_aberto = max(0.0, round(valor_total - valor_pago, 2))
            data_inicio_pagamento = st.date_input("Início dos pagamentos", value=date.today())
            data_lancamento = date.today()

            st.caption(f"💡 Pago até agora: R$ {valor_pago:,.2f} | Em aberto: R$ {valor_em_aberto:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

            if st.form_submit_button("💾 Salvar Empréstimo"):
                try:
                    dados = (
                        str(data_contratacao), valor_total, tipo, banco, int(parcelas_total),
                        int(parcelas_pagas), valor_parcela, taxa_juros_am, int(vencimento_dia),
                        status, st.session_state.usuario_logado["nome"], 
                        str(data_quitacao) if data_quitacao else None,
                        origem_recursos, valor_pago, valor_em_aberto, renegociado_de, descricao,
                        str(data_inicio_pagamento), str(data_lancamento)
                    )
                    repo.salvar_emprestimo(dados)
                    st.session_state["sucesso_emprestimo"] = True
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Erro ao salvar: {e}")

    # ===================== 2) AÇÕES (MEIO) =====================
    st.markdown("---")
    st.markdown("### ✏️ Ações sobre os Empréstimos")

    df = repo.listar_emprestimos()  # deve retornar todas as colunas, incluindo 'id'
    if df.empty:
        st.info("Nenhum empréstimo cadastrado.")
        return

    # Um resumo para a seleção
    df_resumo = df[["id", "data_contratacao", "banco", "valor_total", "status"]].copy()
    df_resumo = df_resumo.sort_values(by="id", ascending=False)
    id_selecionado = st.selectbox("Selecione o ID para editar ou excluir", df_resumo["id"].tolist())

    col_a1, col_a2 = st.columns(2)
    with col_a1:
        if st.button("🗑️ Excluir Empréstimo"):
            repo.excluir_emprestimo(id_selecionado)
            st.success("✅ Empréstimo excluído com sucesso!")
            st.rerun()
    with col_a2:
        if st.button("✏️ Editar Empréstimo"):
            st.session_state["emprestimo_editando"] = id_selecionado
            st.rerun()

    if "emprestimo_editando" in st.session_state:
        id_edit = st.session_state["emprestimo_editando"]
        registro = df[df["id"] == id_edit].iloc[0]

        st.markdown("---")
        st.subheader("✏️ Editar Empréstimo")
        with st.form("form_editar_emprestimo"):
            # Reaproveite os mesmos campos do cadastro, carregando valores atuais:
            col1, col2, col3 = st.columns(3)
            with col1:
                data_contratacao = st.date_input("Data da contratação", value=pd.to_datetime(registro["data_contratacao"]).date())
                valor_total = st.number_input("Valor total (R$)", value=float(registro["valor_total"]), min_value=0.0, step=10.0, format="%.2f")
                tipo = st.selectbox("Tipo", ["Empréstimo", "Financiamento", "Crédito Pessoal", "Outro"], index=0 if registro["tipo"] not in ["Empréstimo","Financiamento","Crédito Pessoal","Outro"] else ["Empréstimo","Financiamento","Crédito Pessoal","Outro"].index(registro["tipo"]))
            with col2:
                banco = st.selectbox("Banco", nomes_bancos, index=nomes_bancos.index(registro["banco"]) if registro["banco"] in nomes_bancos else 0) if nomes_bancos else st.text_input("Banco (digite)", value=str(registro["banco"]))
                parcelas_total = st.number_input("Total de parcelas", value=int(registro["parcelas_total"]), min_value=1, max_value=360, step=1)
                parcelas_pagas = st.number_input("Parcelas pagas", value=int(registro["parcelas_pagas"]), min_value=0, max_value=360, step=1)
            with col3:
                valor_parcela = st.number_input("Valor da parcela (R$)", value=float(registro["valor_parcela"]), min_value=0.0, step=1.0, format="%.2f")
                taxa_juros_am = st.number_input("Juros a.m. (%)", value=float(registro["taxa_juros_am"]), min_value=0.0, step=0.01, format="%.2f")
                vencimento_dia = st.number_input("Dia de vencimento", value=int(registro["vencimento_dia"]), min_value=1, max_value=31, step=1)

            col4, col5, col6 = st.columns(3)
            with col4:
                status = st.selectbox("Status", ["Em aberto", "Quitado", "Renegociado"], index=["Em aberto","Quitado","Renegociado"].index(registro["status"]))
            with col5:
                origem_recursos = st.text_input("Origem dos recursos (opcional)", value=registro.get("origem_recursos","") or "")
                renegociado_de = st.text_input("Renegociado de (ID/descrição) (opcional)", value=registro.get("renegociado_de","") or "")
            with col6:
                descricao = st.text_input("Descrição (opcional)", value=registro.get("descricao","") or "")

            # ❗ Sem checkbox — só mostra calendário se estiver quitado
            data_quitacao = None
            if status == "Quitado":
                valor_atual = registro.get("data_quitacao")
                valor_atual = pd.to_datetime(valor_atual).date() if pd.notna(valor_atual) and str(valor_atual) != "None" else date.today()
                data_quitacao = st.date_input("Data de quitação", value=valor_atual, key="data_quitacao_edit")

            # Recalcula
            valor_pago = round(parcelas_pagas * valor_parcela, 2)
            valor_em_aberto = max(0.0, round(valor_total - valor_pago, 2))
            data_inicio_pagamento = pd.to_datetime(registro["data_inicio_pagamento"]).date() if "data_inicio_pagamento" in registro else date.today()

            if st.form_submit_button("💾 Salvar Edição"):
                try:
                    repo.atualizar_emprestimo(  # implemente no repository se ainda não tiver
                        id_edit,
                        (
                            str(data_contratacao), valor_total, tipo, banco, int(parcelas_total),
                            int(parcelas_pagas), valor_parcela, taxa_juros_am, int(vencimento_dia),
                            status, registro["usuario"], 
                            str(data_quitacao) if data_quitacao else None,
                            origem_recursos, valor_pago, valor_em_aberto, renegociado_de, descricao,
                            str(data_inicio_pagamento), str(registro.get("data_lancamento") or date.today())
                        )
                    )
                    st.success("✅ Empréstimo atualizado!")
                    del st.session_state["emprestimo_editando"]
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Erro ao atualizar: {e}")

    # ===================== 3) REGISTRADOS (FIM DA PÁGINA) =====================
    st.markdown("---")
    st.markdown("### 📋 Empréstimos Registrados (mais recentes primeiro)")

    df_view = df.copy().sort_values("id", ascending=False)
    # Formatações rápidas
    for col in ["valor_total", "valor_parcela", "valor_pago", "valor_em_aberto"]:
        if col in df_view.columns:
            df_view[col] = df_view[col].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
    for col in ["data_contratacao", "data_inicio_pagamento", "data_quitacao", "data_lancamento"]:
        if col in df_view.columns:
            df_view[col] = pd.to_datetime(df_view[col], errors="coerce").dt.strftime("%d/%m/%Y")

    st.dataframe(df_view, use_container_width=True, hide_index=True)