import streamlit as st
from datetime import date
import pandas as pd

from utils.utils import formatar_valor
from .cadastro_classes import CaixaRepository
from repository.movimentacoes_repository import MovimentacoesRepository 


# === Página de Cadastro de Caixa =======================================================================
def pagina_caixa(caminho_banco: str):
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

    # Botão para salvar
    if st.button("💾 Salvar Valores", use_container_width=True):
        try:
            # salva/atualiza e CAPTURA o id/rowid do registro em saldos_caixas
            saldo_id = repo.salvar_saldo(data_caixa_str, valor_final_caixa, valor_final_caixa_2, atualizar)

            # lançamentos em movimentacoes_bancarias via repositório (entrada), amarrando referência
            origem = "saldos_caixas"
            observacao = "Registro manual de caixa"
            referencia_tabela = "saldos_caixas"
            referencia_id = saldo_id if saldo_id and saldo_id > 0 else None

            if valor_novo_caixa and valor_novo_caixa > 0:
                mov_repo.registrar_entrada(
                    data=data_caixa_str,
                    banco="Caixa",
                    valor=float(valor_novo_caixa),
                    origem=origem,
                    observacao=observacao,
                    referencia_tabela=referencia_tabela,
                    referencia_id=referencia_id
                )

            if valor_novo_caixa_2 and valor_novo_caixa_2 > 0:
                mov_repo.registrar_entrada(
                    data=data_caixa_str,
                    banco="Caixa 2",
                    valor=float(valor_novo_caixa_2),
                    origem=origem,
                    observacao=observacao,
                    referencia_tabela=referencia_tabela,
                    referencia_id=referencia_id
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