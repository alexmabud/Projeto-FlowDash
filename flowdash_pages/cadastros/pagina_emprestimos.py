import re
import sqlite3
from datetime import date, datetime
from typing import Optional

import pandas as pd
import streamlit as st

from flowdash_pages.cadastros.cadastro_classes import EmprestimoRepository
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository
from repository.movimentacoes_repository import MovimentacoesRepository
from shared.db import get_conn
from utils.utils import formatar_valor, limpar_valor_formatado

TIPOS_EMPRESTIMO = ["Empréstimo", "Financiamento", "Crédito Pessoal", "Outro"]
STATUS_OPCOES = ["Em aberto", "Quitado", "Renegociado"]


# ===================== helpers de usuário e observação =====================
def _derive_nome_from_email(login: str) -> str:
    base = (login or "").strip()
    if not base:
        return ""
    base = base.split("@", 1)[0]
    base = base.replace(".", " ").replace("_", " ").replace("-", " ")
    return base.strip().title()


def _walk_extract_names(obj) -> str:
    """Percorre dicts/listas e tenta achar campos de nome ou e-mails."""
    try:
        if isinstance(obj, dict):
            for key in ("nome", "name", "display_name", "full_name"):
                v = obj.get(key)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            for key in ("email", "user_email", "usuario_email", "username", "login"):
                v = obj.get(key)
                if isinstance(v, str) and v.strip():
                    nm = _derive_nome_from_email(v)
                    if nm:
                        return nm
            for v in obj.values():
                nm = _walk_extract_names(v)
                if nm:
                    return nm
        elif isinstance(obj, (list, tuple, set)):
            for v in obj:
                nm = _walk_extract_names(v)
                if nm:
                    return nm
        elif isinstance(obj, str):
            if re.search(r".+@.+\..+", obj):
                nm = _derive_nome_from_email(obj)
                if nm:
                    return nm
        return ""
    except Exception:
        return ""


def _resolver_usuario_logado() -> str:
    """Obtém nome do usuário (auth → session_state → email/login → input)."""
    # 1) Tenta módulo auth, se existir
    try:
        from auth import auth as _auth
        for fn_name in ("get_usuario_logado", "usuario_logado", "get_current_user"):
            fn = getattr(_auth, fn_name, None)
            if callable(fn):
                val = fn()
                nm = _walk_extract_names(val) if not isinstance(val, str) else (val.strip() or "")
                if nm:
                    return nm
        for attr in ("usuario_atual", "current_user", "usuario"):
            val = getattr(_auth, attr, None)
            nm = _walk_extract_names(val) if not isinstance(val, str) else (val.strip() or "")
            if nm:
                return nm
    except Exception:
        pass

    # 2) Chaves diretas no session_state
    for k in [
        "nome_usuario", "usuario_nome", "user_nome", "user_name",
        "nome", "nomeCompleto", "usuario_atual_nome", "current_user_name",
        "display_name", "full_name",
    ]:
        v = st.session_state.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # 3) Estruturas aninhadas / varredura completa
    for k in ["usuario", "user", "current_user", "auth_user", "logged_user", "auth"]:
        nm = _walk_extract_names(st.session_state.get(k))
        if nm:
            return nm
    for k in list(st.session_state.keys()):
        nm = _walk_extract_names(st.session_state.get(k))
        if nm:
            return nm

    # 4) Fallback por email/login
    for k in ("usuario_email", "user_email", "email", "login", "username"):
        v = st.session_state.get(k)
        if isinstance(v, str) and v.strip():
            nm = _derive_nome_from_email(v)
            if nm:
                return nm

    # 5) Confirmação na UI
    usuario_atual = st.text_input(
        "Usuário logado",
        value=st.session_state.get("usuario_confirmado", ""),
        placeholder="Digite seu nome para registrar nos lançamentos",
        help="Não consegui capturar seu nome automaticamente. Confirme aqui para gravar corretamente."
    ).strip()
    if usuario_atual:
        st.session_state["usuario_confirmado"] = usuario_atual
    return usuario_atual


def _obs_deposito_padrao(valor: float, extra: str = "") -> str:
    """Observação padronizada para depósito de empréstimo."""
    obs = f"Cadastro REGISTRO MANUAL DE DEPÓSITO DE EMPRÉSTIMO | Valor {formatar_valor(valor)}"
    return f"{obs} | {extra}" if extra else obs
# ========================================================================


# =============================== I/O banco ===============================
def carregar_bancos_cadastrados(caminho_banco: str) -> pd.DataFrame:
    """Lê tabela bancos_cadastrados (id, nome)."""
    with sqlite3.connect(caminho_banco) as conn:
        return pd.read_sql("SELECT id, nome FROM bancos_cadastrados ORDER BY nome", conn)


def inserir_movimentacao_bancaria(
    caminho_banco: str,
    data_: str,
    banco_nome: str,
    valor: float,
    emprestimo_id: int,
    observacao: str = "",
    *,
    usuario: Optional[str] = None,
    data_hora: Optional[str] = None,
):
    """Registra ENTRADA em movimentacoes_bancarias (origem='emprestimo')."""
    try:
        if not banco_nome or valor is None or float(valor) <= 0:
            return None

        observacao_final = _obs_deposito_padrao(valor, extra=observacao or "")
        mov_repo = MovimentacoesRepository(caminho_banco)
        mov_id = mov_repo.registrar_entrada(
            data=str(data_),
            banco=str(banco_nome).strip(),
            valor=float(valor),
            origem="emprestimo",
            observacao=observacao_final,
            referencia_tabela="emprestimos",
            referencia_id=int(emprestimo_id) if emprestimo_id else None,
            usuario=usuario,
            data_hora=data_hora,
        )
        return mov_id
    except Exception as e:
        st.warning(f"Não foi possível registrar a movimentação bancária do empréstimo: {e}")
        return None
# ========================================================================


def pagina_emprestimos_financiamentos(caminho_banco: str):
    """Página de Cadastro e Gestão de Empréstimos / Financiamentos."""
    st.subheader("🏦 Cadastro de Empréstimos e Financiamentos")
    repo = EmprestimoRepository(caminho_banco)

    if st.session_state.get("sucesso_emprestimo"):
        st.success("✅ Empréstimo salvo com sucesso!")
        del st.session_state["sucesso_emprestimo"]

    # ----------------------- Cadastrar novo empréstimo -----------------------
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

            # 🔐 usuário do cadastro (agora usando o mesmo resolver)
            usuario_cadastro = _resolver_usuario_logado()
            if not usuario_cadastro:
                st.warning("⚠️ Confirme o campo **Usuário logado** acima para salvar o empréstimo.")
            data_lancamento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            submit = st.form_submit_button("📋 Salvar Empréstimo", disabled=(not usuario_cadastro))

            if submit:
                try:
                    if parcelas_pagas > parcelas_total:
                        st.warning("⚠️ Parcelas pagas maior que o total de parcelas. Verifique se houve adiantamento ou erro.")

                    valor_pago = valor_parcela * parcelas_pagas
                    valor_em_aberto = valor_total - valor_pago

                    dados = (
                        str(data_contratacao), valor_total, tipo, banco, int(parcelas_total),
                        int(parcelas_pagas), valor_parcela, taxa_juros_am, int(vencimento_dia),
                        status, usuario_cadastro, str(data_quitacao) if data_quitacao else None, origem,
                        valor_pago, valor_em_aberto, None, descricao,
                        str(data_inicio_pagamento), data_lancamento
                    )

                    # 1) Salva o cadastro
                    repo.salvar_emprestimo(dados)
                    st.success("✅ Empréstimo salvo com sucesso!")

                    # 2) Descobre o id recém-inserido e programa as parcelas no CAP (mesma conexão)
                    cap_repo = ContasAPagarMovRepository(caminho_banco)
                    with get_conn(caminho_banco) as conn:
                        row = conn.execute(
                            "SELECT id FROM emprestimos_financiamentos ORDER BY id DESC LIMIT 1"
                        ).fetchone()
                        if not row or row[0] is None:
                            st.error("Não foi possível obter o ID do empréstimo recém-cadastrado.")
                        else:
                            novo_id = int(row[0])
                            try:
                                resultado = cap_repo.gerar_parcelas_emprestimo(
                                    conn,
                                    emprestimo_id=novo_id,
                                    usuario=usuario_cadastro
                                )
                                st.success(
                                    f"🧾 Parcelas programadas no Contas a Pagar: {resultado.get('criadas', 0)} "
                                    f"(ajustes: {resultado.get('ajustes_quitadas', 0)})"
                                )
                            except Exception as e:
                                st.error(f"Falha ao programar parcelas no Contas a Pagar: {e}")

                    # 3) Limpa inputs do form
                    for chave in list(st.session_state.keys()):
                        if chave.startswith("input_"):
                            del st.session_state[chave]

                except Exception as e:
                    st.error(f"Erro ao salvar: {e}")

    # ---------------------------- Listagem + ações ----------------------------
    st.markdown("### 📋 Empréstimos Registrados")
    try:
        df = repo.listar_emprestimos()

        if not df.empty:
            df["Situação"] = df.apply(
                lambda row: "Novo" if row["parcelas_pagas"] == 0 else (
                    "Quitado" if row["parcelas_pagas"] >= row["parcelas_total"] else "Em andamento"
                ),
                axis=1
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

            # Registrar Depósito (usa id_selecionado)
            with st.expander("🏦 Registrar depósito deste empréstimo em um banco", expanded=False):
                try:
                    df_bancos = carregar_bancos_cadastrados(caminho_banco)
                    if df_bancos.empty:
                        st.warning("⚠️ Nenhum banco cadastrado em `bancos_cadastrados`.")
                    else:
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            data_deposito = st.date_input("Data do depósito", value=date.today(), key=f"dep_data_{id_selecionado}")
                        with c2:
                            banco_nome = st.selectbox(
                                "Banco destino",
                                df_bancos["nome"].tolist(),
                                key=f"dep_banco_{id_selecionado}"
                            )
                        with c3:
                            valor_deposito = st.number_input(
                                "Valor (R$)",
                                min_value=0.0, step=10.0, format="%.2f",
                                key=f"dep_valor_{id_selecionado}"
                            )

                        observacao_extra = st.text_input(
                            "Observação (opcional)",
                            value=f"Depósito do empréstimo ID {id_selecionado}",
                            key=f"dep_obs_{id_selecionado}"
                        )

                        # 🔐 usuário + timestamp (mesma regra do cadastro)
                        usuario_dep = _resolver_usuario_logado()
                        if not usuario_dep:
                            st.warning("⚠️ Confirme o campo **Usuário logado** acima para continuar.")
                        agora_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        if st.button("💾 Salvar saldo bancário", key=f"btn_dep_{id_selecionado}", disabled=(not usuario_dep)):
                            if valor_deposito <= 0:
                                st.warning("Informe um valor válido.")
                            else:
                                try:
                                    inserir_movimentacao_bancaria(
                                        caminho_banco=caminho_banco,
                                        data_=str(data_deposito),
                                        banco_nome=banco_nome,
                                        valor=valor_deposito,
                                        emprestimo_id=id_selecionado,
                                        observacao=observacao_extra,
                                        usuario=usuario_dep,
                                        data_hora=agora_str,
                                    )
                                    st.success("✅ Depósito registrado em `movimentacoes_bancarias`!")
                                except Exception as e:
                                    st.error(f"Erro ao registrar depósito: {e}")
                except Exception as e:
                    st.error(f"Erro ao carregar bancos: {e}")

        # MODO EDIÇÃO
        if "emprestimo_editando" in st.session_state and not df.empty:
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
                    data_quitacao = st.date_input("Data da Última Parcela (Quitação)", value=pd.to_datetime(emprestimo["data_quitacao"]) if emprestimo["data_quitacao"] else None)
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
        elif df.empty:
            st.info("Nenhum empréstimo registrado ainda.")

    except Exception as e:
        st.error(f"Erro ao carregar empréstimos: {e}")
