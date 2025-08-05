import streamlit as st
import pandas as pd
from datetime import datetime
from utils.utils import formatar_valor, formatar_percentual
from flowdash_pages.cadastros.cadastro_classes import MetaManager, DIAS_SEMANA

# Página de Cadastro de Metas =====================================================================================
def pagina_metas_cadastro(caminho_banco: str):
    st.markdown("## 🎯 Cadastro de Metas")
    manager = MetaManager(caminho_banco)

    try:
        lista_usuarios = manager.carregar_usuarios_ativos()
    except Exception as e:
        st.error(f"Erro ao carregar usuários: {e}")
        return

    nomes = [nome for nome, _ in lista_usuarios]
    vendedor_selecionado = st.selectbox("Selecione o usuário para cadastro de meta", nomes)
    id_usuario = dict(lista_usuarios)[vendedor_selecionado]
    mes_atual = datetime.today().strftime("%Y-%m")
    st.markdown(f"#### 📆 Mês Atual: `{mes_atual}`")

    st.markdown("### 💰 Meta Mensal")
    meta_mensal = st.number_input("Valor da meta mensal (R$)", min_value=0.0, step=100.0, format="%.2f")

    st.markdown("### 🧮 Metas Prata e Bronze")
    perc_prata = st.number_input("Percentual Prata (%)", 0.0, 100.0, 87.5, 0.5, format="%.1f")
    perc_bronze = st.number_input("Percentual Bronze (%)", 0.0, 100.0, 75.0, 0.5, format="%.1f")

    st.info(f"Meta Prata: {formatar_valor(meta_mensal * perc_prata / 100)}")
    st.info(f"Meta Bronze: {formatar_valor(meta_mensal * perc_bronze / 100)}")

    st.markdown("### 📅 Meta Semanal")
    semanal_percentual = st.number_input("Percentual da meta mensal para a meta semanal (%)", 0.0, 100.0, 25.0, 1.0, format="%.1f")
    meta_semanal_valor = meta_mensal * (semanal_percentual / 100)
    st.success(f"Meta Semanal: {formatar_valor(meta_semanal_valor)}")

    st.markdown("### 📆 Distribuição Diária (% da meta semanal)")
    col1, col2, col3 = st.columns(3)
    percentuais = []
    for i, dia in enumerate(DIAS_SEMANA):
        col = [col1, col2, col3][i % 3]
        with col:
            p = st.number_input(f"{dia} (%)", 0.0, 100.0, 0.0, 1.0, format="%.1f")
            percentuais.append(p)
            st.caption(f"→ {formatar_valor(meta_semanal_valor * (p / 100))}")

    if st.button("💾 Salvar Metas"):
        if round(sum(percentuais), 2) != 100.0:
            st.warning(f"A soma dos percentuais diários deve ser 100%. Está em {sum(percentuais):.2f}%")
        else:
            try:
                sucesso = manager.salvar_meta(
                    id_usuario=id_usuario,
                    vendedor=vendedor_selecionado,
                    mensal=meta_mensal,
                    semanal_percentual=semanal_percentual,
                    dias_percentuais=percentuais,
                    perc_bronze=perc_bronze,
                    perc_prata=perc_prata,
                    mes=mes_atual
                )
                if sucesso:
                    st.success("✅ Metas salvas com sucesso!")
                    st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar metas: {e}")

    st.divider()
    st.markdown("### 📋 Todas as Metas Cadastradas")
    try:
        metas = manager.carregar_metas_cadastradas()
        if metas:
            df = pd.DataFrame(metas)
            df["Meta Mensal"] = df["Meta Mensal"].apply(formatar_valor)
            df["Meta Semanal"] = df["Meta Semanal"].apply(formatar_percentual)
            df["% Prata"] = df["% Prata"].apply(formatar_percentual)
            df["% Bronze"] = df["% Bronze"].apply(formatar_percentual)
            for dia in DIAS_SEMANA:
                df[dia] = df[dia].apply(formatar_percentual)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhuma meta cadastrada ainda.")
    except Exception as e:
        st.error(f"Erro ao exibir metas: {e}")