import streamlit as st
import pandas as pd
from datetime import date, datetime
from flowdash_pages.cadastros.cadastro_classes import EmprestimoRepository
from utils.utils import formatar_valor, limpar_valor_formatado

TIPOS_EMPRESTIMO = ["Empréstimo", "Financiamento", "Crédito Pessoal", "Outro"]
STATUS_OPCOES = ["Em aberto", "Quitado", "Renegociado"]

def pagina_emprestimos_financiamentos(caminho_banco: str):
    st.subheader("🏦 Cadastro de Empréstimos e Financiamentos")
    repo = EmprestimoRepository(caminho_banco)

    if st.session_state.get("sucesso_emprestimo"):
        st.success("✅ Empréstimo salvo com sucesso!")
        del st.session_state["sucesso_emprestimo"]

    with st.expander("➕ Cadastrar Novo Empréstimo / Financiamento", expanded=True):
        with st.form("form_emprestimo"):
            # Linha 1 - Datas
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                data_contratacao = st.date_input("Data da Contratação", value=date.today(), key="input_data")
            with col2:
                data_inicio_pagamento = st.date_input("Data de Início do Pagamento", value=date.today(), key="input_inicio_pagamento")
            with col3:
                data_quitacao = st.date_input("Data da Última Parcela)", value=None, key="input_data_quitacao")
            with col4:
                vencimento_dia = st.number_input("Dia do Vencimento", min_value=1, max_value=31, step=1, key="input_vencimento")

            # Linha 2 - Valores
            col5, col6, col7, col8 = st.columns(4)
            with col5:
                valor_total = st.number_input("Valor Total", min_value=0.0, step=100.0, format="%.2f", key="input_valor_total")
            with col6:
                parcelas_total = st.number_input("Parcelas Totais", min_value=1, step=1, key="input_parcelas_total")
            with col7:
                valor_parcela = st.number_input("Valor da Parcela", min_value=0.0, step=10.0, format="%.2f", key="input_valor_parcela")
            with col8:
                parcelas_pagas = st.number_input("Parcelas Já Pagas", min_value=0, step=1, key="input_parcelas_pagas")

            # Linha 3 - Detalhes
            col9, col10, col11, col12 = st.columns(4)
            with col9:
                tipo = st.selectbox("Tipo", TIPOS_EMPRESTIMO, key="input_tipo")
            with col10:
                taxa_juros_am = st.number_input("Taxa de Juros (% a.m.)", min_value=0.0, step=0.01, format="%.2f", key="input_juros")
            with col11:
                banco = st.text_input("Banco ou Instituição", key="input_banco")
            with col12:
                status = st.selectbox("Status", STATUS_OPCOES, key="input_status")

            # Linha 4 - Descrição e Origem
            col13, col14 = st.columns(2)
            with col13:
                descricao = st.text_area("Descrição", key="input_descricao")
            with col14:
                origem = st.text_input("Origem dos Recursos", key="input_origem")

            usuario = st.session_state.get("usuario_logado", {}).get("nome", "Sistema")

            submit = st.form_submit_button("📋 Salvar Empréstimo")

            if submit:
                try:
                    if parcelas_pagas > parcelas_total:
                        st.warning("⚠️ Parcelas pagas maior que o total de parcelas. Verifique se houve adiantamento ou erro.")

                    valor_pago = valor_parcela * parcelas_pagas
                    valor_em_aberto = valor_total - valor_pago
                    data_lancamento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    dados = (
                        str(data_contratacao), valor_total, tipo, banco, int(parcelas_total),
                        int(parcelas_pagas), valor_parcela, taxa_juros_am, int(vencimento_dia),
                        status, usuario, str(data_quitacao) if data_quitacao else None, origem,
                        valor_pago, valor_em_aberto, None, descricao,
                        str(data_inicio_pagamento), data_lancamento
                    )
                    repo.salvar_emprestimo(dados)

                    st.success("✅ Empréstimo salvo com sucesso!")

                    for chave in list(st.session_state.keys()):
                        if chave.startswith("input_"):
                            del st.session_state[chave]

                except Exception as e:
                    st.error(f"Erro ao salvar: {e}")
import streamlit as st
import pandas as pd
from datetime import date, datetime
from flowdash_pages.cadastros.cadastro_classes import EmprestimoRepository
from utils.utils import formatar_valor, limpar_valor_formatado

TIPOS_EMPRESTIMO = ["Empréstimo", "Financiamento", "Crédito Pessoal", "Outro"]
STATUS_OPCOES = ["Em aberto", "Quitado", "Renegociado"]

def pagina_emprestimos_financiamentos(caminho_banco: str):
    st.subheader("🏦 Cadastro de Empréstimos e Financiamentos")
    repo = EmprestimoRepository(caminho_banco)

    if st.session_state.get("sucesso_emprestimo"):
        st.success("✅ Empréstimo salvo com sucesso!")
        del st.session_state["sucesso_emprestimo"]

    with st.expander("➕ Cadastrar Novo Empréstimo / Financiamento", expanded=True):
        with st.form("form_emprestimo"):
            # Linha 1 - Datas
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                data_contratacao = st.date_input("Data da Contratação", value=date.today(), key="input_data")
            with col2:
                data_inicio_pagamento = st.date_input("Data de Início do Pagamento", value=date.today(), key="input_inicio_pagamento")
            with col3:
                data_quitacao = st.date_input("Data da Última Parcela", value=None, key="input_data_quitacao")
            with col4:
                vencimento_dia = st.number_input("Dia do Vencimento", min_value=1, max_value=31, step=1, key="input_vencimento")

            # Linha 2 - Valores
            col5, col6, col7, col8 = st.columns(4)
            with col5:
                valor_total = st.number_input("Valor Total", min_value=0.0, step=100.0, format="%.2f", key="input_valor_total")
            with col6:
                parcelas_total = st.number_input("Parcelas Totais", min_value=1, step=1, key="input_parcelas_total")
            with col7:
                valor_parcela = st.number_input("Valor da Parcela", min_value=0.0, step=10.0, format="%.2f", key="input_valor_parcela")
            with col8:
                parcelas_pagas = st.number_input("Parcelas Já Pagas", min_value=0, step=1, key="input_parcelas_pagas")

            # Linha 3 - Detalhes
            col9, col10, col11, col12 = st.columns(4)
            with col9:
                tipo = st.selectbox("Tipo", TIPOS_EMPRESTIMO, key="input_tipo")
            with col10:
                taxa_juros_am = st.number_input("Taxa de Juros (% a.m.)", min_value=0.0, step=0.01, format="%.2f", key="input_juros")
            with col11:
                banco = st.text_input("Banco ou Instituição", key="input_banco")
            with col12:
                status = st.selectbox("Status", STATUS_OPCOES, key="input_status")

            # Linha 4 - Descrição e Origem
            col13, col14 = st.columns(2)
            with col13:
                descricao = st.text_area("Descrição", key="input_descricao")
            with col14:
                origem = st.text_input("Origem dos Recursos", key="input_origem")

            usuario = st.session_state.get("usuario_logado", {}).get("nome", "Sistema")

            submit = st.form_submit_button("📋 Salvar Empréstimo")

            if submit:
                try:
                    if parcelas_pagas > parcelas_total:
                        st.warning("⚠️ Parcelas pagas maior que o total de parcelas. Verifique se houve adiantamento ou erro.")

                    valor_pago = valor_parcela * parcelas_pagas
                    valor_em_aberto = valor_total - valor_pago
                    data_lancamento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    dados = (
                        str(data_contratacao), valor_total, tipo, banco, int(parcelas_total),
                        int(parcelas_pagas), valor_parcela, taxa_juros_am, int(vencimento_dia),
                        status, usuario, str(data_quitacao) if data_quitacao else None, origem,
                        valor_pago, valor_em_aberto, None, descricao,
                        str(data_inicio_pagamento), data_lancamento
                    )
                    repo.salvar_emprestimo(dados)

                    st.success("✅ Empréstimo salvo com sucesso!")

                    for chave in list(st.session_state.keys()):
                        if chave.startswith("input_"):
                            del st.session_state[chave]

                except Exception as e:
                    st.error(f"Erro ao salvar: {e}")

    st.markdown("### 📋 Empréstimos Registrados")
    try:
        df = repo.listar_emprestimos()
        if not df.empty:
            df["Situação"] = df.apply(
                lambda row: "Novo" if row["parcelas_pagas"] == 0 else (
                    "Quitado" if row["parcelas_pagas"] >= row["parcelas_total"] else "Em andamento"
                ), axis=1
            )
            df["valor_total"] = df["valor_total"].apply(formatar_valor)
            df["valor_parcela"] = df["valor_parcela"].apply(lambda x: formatar_valor(x) if pd.notnull(x) else "")
            df["valor_em_aberto"] = df["valor_em_aberto"].apply(lambda x: formatar_valor(x) if pd.notnull(x) else "")
            df["valor_pago"] = df["valor_pago"].apply(lambda x: formatar_valor(x) if pd.notnull(x) else "")
            df["data_contratacao"] = pd.to_datetime(df["data_contratacao"]).dt.strftime("%d/%m/%Y")

            colunas_exibir = [
                "id", "data_contratacao", "tipo", "banco", "valor_total",
                "parcelas_total", "parcelas_pagas", "valor_parcela", "Situação"
            ]
            df_resumo = df[colunas_exibir].rename(columns={
                "data_contratacao": "Data",
                "tipo": "Tipo",
                "banco": "Banco",
                "valor_total": "Valor Total",
                "parcelas_total": "Parcelas",
                "parcelas_pagas": "Pagas",
                "valor_parcela": "Valor Parcela"
            })

            st.dataframe(df_resumo, use_container_width=True, hide_index=True)

            st.markdown("### ✏️ Ações sobre os Empréstimos")
            id_selecionado = st.selectbox("Selecione o ID para editar ou excluir", df_resumo["id"].tolist())

            col1, col2 = st.columns(2)
            with col1:
                if st.button("🗑️ Excluir Empréstimo"):
                    repo.excluir_emprestimo(id_selecionado)
                    st.success("✅ Empréstimo excluído com sucesso!")
                    st.rerun()
            with col2:
                if st.button("✏️ Editar Empréstimo"):
                    st.session_state["emprestimo_editando"] = id_selecionado
                    st.rerun()

        if "emprestimo_editando" in st.session_state:
            id_edit = st.session_state["emprestimo_editando"]
            emprestimo = df[df["id"] == id_edit].iloc[0]

            st.markdown("---")
            st.subheader("✏️ Editar Empréstimo")
            with st.form("form_editar_emprestimo"):
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    data_contratacao = st.date_input("Data da Contratação", value=pd.to_datetime(emprestimo["data_contratacao"]))
                with col2:
                    data_inicio_pagamento = st.date_input("Data de Início do Pagamento", value=pd.to_datetime(emprestimo["data_inicio_pagamento"]))
                with col3:
                    data_quitacao = st.date_input("Data da Última Parcela (Quitacão)", value=pd.to_datetime(emprestimo["data_quitacao"]) if emprestimo["data_quitacao"] else None)
                with col4:
                    vencimento_dia = st.number_input("Dia do Vencimento", value=int(emprestimo["vencimento_dia"]))

                col5, col6, col7, col8 = st.columns(4)
                with col5:
                    valor_total = st.number_input("Valor Total", value=limpar_valor_formatado(emprestimo["valor_total"]))
                with col6:
                    parcelas_total = st.number_input("Parcelas Totais", value=int(emprestimo["parcelas_total"]))
                with col7:
                    valor_parcela = st.number_input("Valor da Parcela", value=limpar_valor_formatado(emprestimo["valor_parcela"]))
                with col8:
                    parcelas_pagas = st.number_input("Parcelas Já Pagas", value=int(emprestimo["parcelas_pagas"]))

                col9, col10, col11, col12 = st.columns(4)
                with col9:
                    tipo = st.selectbox("Tipo", TIPOS_EMPRESTIMO, index=TIPOS_EMPRESTIMO.index(emprestimo["tipo"]))
                with col10:
                    taxa_juros_am = st.number_input("Taxa de Juros (% a.m.)", value=limpar_valor_formatado(emprestimo["taxa_juros_am"]))
                with col11:
                    banco = st.text_input("Banco", emprestimo["banco"])
                with col12:
                    status = st.selectbox("Status", STATUS_OPCOES, index=STATUS_OPCOES.index(emprestimo["status"]))

                col13, col14 = st.columns(2)
                with col13:
                    descricao = st.text_area("Descrição", value=emprestimo["descricao"])
                with col14:
                    origem = st.text_input("Origem dos Recursos", value=emprestimo["origem_recursos"])

                usuario = emprestimo["usuario"]
                data_lancamento = emprestimo["data_lancamento"]
                valor_pago = valor_parcela * parcelas_pagas
                valor_em_aberto = valor_total - valor_pago

                if st.form_submit_button("✅ Atualizar"):
                    novos_dados = {
                        "data_contratacao": str(data_contratacao),
                        "valor_total": valor_total,
                        "tipo": tipo,
                        "banco": banco,
                        "parcelas_total": int(parcelas_total),
                        "parcelas_pagas": int(parcelas_pagas),
                        "valor_parcela": valor_parcela,
                        "taxa_juros_am": taxa_juros_am,
                        "vencimento_dia": int(vencimento_dia),
                        "status": status,
                        "usuario": usuario,
                        "data_quitacao": str(data_quitacao) if data_quitacao else None,
                        "origem_recursos": origem,
                        "valor_pago": valor_pago,
                        "valor_em_aberto": valor_em_aberto,
                        "renegociado_de": None,
                        "descricao": descricao,
                        "data_inicio_pagamento": str(data_inicio_pagamento),
                        "data_lancamento": data_lancamento
                    }
                    repo.editar_emprestimo(id_edit, novos_dados)
                    del st.session_state["emprestimo_editando"]
                    st.success("✅ Atualizado com sucesso!")
                    st.rerun()
        else:
            st.info("Nenhum empréstimo registrado ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar empréstimos: {e}")
