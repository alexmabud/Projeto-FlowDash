"""
Página: Cadastro de Caixa
=========================

Responsável por cadastrar/atualizar os saldos de **Caixa** e **Caixa 2** na data
selecionada, bem como registrar os lançamentos correspondentes em
`movimentacoes_bancarias` via repositório, com observação padronizada e inclusão
de metadados de usuário e timestamp.

Principais comportamentos
-------------------------
- Mostra saldos já cadastrados na data e permite somar novos valores.
- Persiste o snapshot do dia em `saldos_caixas` (via `CaixaRepository`).
- Registra entradas em `movimentacoes_bancarias` com:
  - observação: "Cadastro REGISTRO MANUAL DE CAIXA | Valor R$ X"
  - `usuario` = nome do usuário logado (resolvido automaticamente; se não for possível, pede confirmação na UI)
  - `data_hora` = timestamp atual (YYYY-MM-DD HH:MM:SS)
- Exibe uma lista dos últimos registros (saldos) formatados.
"""

import re
import streamlit as st
from datetime import date, datetime
import pandas as pd

from utils.utils import formatar_valor
from .cadastro_classes import CaixaRepository
from repository.movimentacoes_repository import MovimentacoesRepository


def pagina_caixa(caminho_banco: str) -> None:
    """Renderiza a página de **Cadastro de Caixa**.

    Esta função permite selecionar uma data de referência, visualizar os saldos
    existentes de Caixa e Caixa 2, somar novos valores e salvar o resultado
    (snapshot do dia) no banco. Além disso, registra automaticamente as entradas
    na tabela `movimentacoes_bancarias` com observação padronizada e metadados
    de usuário e timestamp.

    Args:
        caminho_banco (str): Caminho absoluto ou relativo para o arquivo SQLite.
    """
    st.subheader("💰 Cadastro de Caixa")
    repo = CaixaRepository(caminho_banco)
    mov_repo = MovimentacoesRepository(caminho_banco)

    # --- feedback pós-rerun (mostra mensagem salva antes do st.rerun)
    if st.session_state.get("caixa_msg_sucesso"):
        st.success(st.session_state.pop("caixa_msg_sucesso"))

    # Seleção da data
    data_caixa = st.date_input("Data de Referência", value=date.today())
    data_caixa_str = str(data_caixa)

    # Busca saldos existentes
    resultado = repo.buscar_saldo_por_data(data_caixa_str)

    if resultado:
        caixa_atual = resultado[0] if not isinstance(resultado, dict) else resultado.get("caixa", 0)
        caixa2_atual = resultado[1] if not isinstance(resultado, dict) else resultado.get("caixa_2", 0)

        st.info(
            f"🔄 Valores já cadastrados para `{data_caixa_str}`:\n\n"
            f"- 💵 **Caixa (loja)**: R$ {caixa_atual:.2f}\n"
            f"- 🏠 **Caixa 2 (casa)**: R$ {caixa2_atual:.2f}\n\n"
            f"📌 O valor digitado abaixo será **somado** a esses saldos."
        )

        valor_novo_caixa = st.number_input("Adicionar ao Caixa", min_value=0.0, step=10.0, format="%.2f")
        valor_novo_caixa_2 = st.number_input("Adicionar ao Caixa 2", min_value=0.0, step=10.0, format="%.2f")

        valor_final_caixa = caixa_atual + valor_novo_caixa
        valor_final_caixa_2 = caixa2_atual + valor_novo_caixa_2
        atualizar = True
    else:
        st.warning("⚠️ Nenhum valor cadastrado para essa data. Informe o valor inicial.")
        valor_final_caixa = st.number_input("Caixa", min_value=0.0, step=10.0, format="%.2f")
        valor_final_caixa_2 = st.number_input("Caixa 2", min_value=0.0, step=10.0, format="%.2f")
        valor_novo_caixa = valor_final_caixa
        valor_novo_caixa_2 = valor_final_caixa_2
        atualizar = False

    # ------------------- resolver usuário e timestamp -------------------
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
                # prioridade: chaves de nome
                for key in ("nome", "name", "display_name", "full_name"):
                    v = obj.get(key)
                    if isinstance(v, str) and v.strip():
                        return v.strip()
                # fallback: alguma chave com email
                for key in ("email", "user_email", "usuario_email", "username", "login"):
                    v = obj.get(key)
                    if isinstance(v, str) and v.strip():
                        nm = _derive_nome_from_email(v)
                        if nm:
                            return nm
                # varre recursivamente
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
                # string que pode ser email
                if re.search(r".+@.+\..+", obj):
                    nm = _derive_nome_from_email(obj)
                    if nm:
                        return nm
            return ""
        except Exception:
            return ""

    def _usuario_logado() -> str:
        """Obtém o nome do usuário logado (session_state/auth) ou pede confirmação."""
        # 1) Tentativa via módulo auth (se existir)
        try:
            from auth import auth as _auth  # Projeto FlowDash/auth/auth.py
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

        # 2) Chaves diretas
        for k in [
            "nome_usuario", "usuario_nome", "user_nome", "user_name",
            "nome", "nomeCompleto", "usuario_atual_nome", "current_user_name",
            "display_name", "full_name",
        ]:
            v = st.session_state.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

        # 3) Objetos/dicts aninhados + varredura completa do session_state
        for k in ["usuario", "user", "current_user", "auth_user", "logged_user", "auth"]:
            nm = _walk_extract_names(st.session_state.get(k))
            if nm:
                return nm

        # varredura em todas as chaves do session_state
        for k in list(st.session_state.keys()):
            nm = _walk_extract_names(st.session_state.get(k))
            if nm:
                return nm

        # 4) Derivar de email/login em qualquer chave string
        for k in ("usuario_email", "user_email", "email", "login", "username"):
            v = st.session_state.get(k)
            if isinstance(v, str) and v.strip():
                nm = _derive_nome_from_email(v)
                if nm:
                    return nm

        return ""  # retorna vazio para que a UI peça confirmação

    usuario_atual = _usuario_logado()
    # Se ainda não temos usuário, peça confirmação explícita na UI
    if not usuario_atual:
        usuario_atual = st.text_input(
            "Usuário logado",
            value=st.session_state.get("usuario_confirmado", ""),
            placeholder="Digite seu nome para registrar nos lançamentos",
            help="Não consegui capturar seu nome automaticamente. Confirme aqui para gravar corretamente."
        ).strip()
        if usuario_atual:
            st.session_state["usuario_confirmado"] = usuario_atual

    if not usuario_atual:
        # Evita salvar como 'desconhecido'
        st.warning("⚠️ Confirme o campo **Usuário logado** acima para continuar.")
    agora_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # --------------------------------------------------------------------

    # Botão para salvar
    disabled_save = not bool(usuario_atual)  # bloqueia salvar sem usuário
    if st.button("💾 Salvar Valores", use_container_width=True, disabled=disabled_save):
        try:
            # salva/atualiza e CAPTURA o id/rowid do registro em saldos_caixas
            saldo_id = repo.salvar_saldo(data_caixa_str, valor_final_caixa, valor_final_caixa_2, atualizar)

            # lançamentos em movimentacoes_bancarias via repositório (entrada), amarrando referência
            origem = "saldos_caixas"
            referencia_tabela = "saldos_caixas"
            referencia_id = saldo_id if saldo_id and saldo_id > 0 else None

            # ===== observação padronizada =====
            obs_caixa  = f"Cadastro REGISTRO MANUAL DE CAIXA | Valor {formatar_valor(valor_novo_caixa)}"
            obs_caixa2 = f"Cadastro REGISTRO MANUAL DE CAIXA | Valor {formatar_valor(valor_novo_caixa_2)}"
            # ==================================

            if valor_novo_caixa and valor_novo_caixa > 0:
                mov_repo.registrar_entrada(
                    data=data_caixa_str,
                    banco="Caixa",
                    valor=float(valor_novo_caixa),
                    origem=origem,
                    observacao=obs_caixa,
                    referencia_tabela=referencia_tabela,
                    referencia_id=referencia_id,
                    usuario=usuario_atual,
                    data_hora=agora_str,
                )

            if valor_novo_caixa_2 and valor_novo_caixa_2 > 0:
                mov_repo.registrar_entrada(
                    data=data_caixa_str,
                    banco="Caixa 2",
                    valor=float(valor_novo_caixa_2),
                    origem=origem,
                    observacao=obs_caixa2,
                    referencia_tabela=referencia_tabela,
                    referencia_id=referencia_id,
                    usuario=usuario_atual,
                    data_hora=agora_str,
                )

            # Mensagens no formato solicitado
            msgs = []
            if valor_novo_caixa > 0:
                msgs.append(f"Valor {formatar_valor(valor_novo_caixa)} salvo em caixa")
            if valor_novo_caixa_2 > 0:
                msgs.append(f"Valor {formatar_valor(valor_novo_caixa_2)} salvo em caixa 2")

            st.session_state["caixa_msg_sucesso"] = " | ".join(msgs) if msgs else "Nenhum valor informado."
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar: {e}")

    st.markdown("---")
    st.markdown("### 📋 Últimos Registros")

    # Visualização dos últimos saldos
    try:
        df_caixa = repo.listar_ultimos_saldos()
        if not df_caixa.empty:
            # detectar coluna de vendas, se existir
            col_vendas = "caixa_vendas" if "caixa_vendas" in df_caixa.columns else ("caixa_venda" if "caixa_venda" in df_caixa.columns else None)

            df_caixa["data"] = pd.to_datetime(df_caixa["data"]).dt.strftime("%d/%m/%Y")

            # formata colunas monetárias existentes (inclui a coluna de vendas detectada, se houver)
            colunas_monetarias = ["caixa", "caixa_2", "caixa_total", "caixa2_dia", "caixa2_total"]
            if col_vendas:
                colunas_monetarias.append(col_vendas)

            for col in colunas_monetarias:
                if col in df_caixa.columns:
                    df_caixa[col] = df_caixa[col].apply(formatar_valor)

            # exibe somente colunas que existem
            colunas_exibir = ["data", "caixa", "caixa_total", "caixa_2", "caixa2_dia", "caixa2_total"]
            if col_vendas:
                colunas_exibir.insert(2, col_vendas)  # depois de 'caixa'

            colunas_exibir = [c for c in colunas_exibir if c in df_caixa.columns]
            st.dataframe(df_caixa[colunas_exibir], use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum dado cadastrado ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar: {e}")
