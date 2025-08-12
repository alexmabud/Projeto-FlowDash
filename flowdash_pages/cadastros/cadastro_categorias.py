import streamlit as st
import pandas as pd
from repository.categorias_repository import CategoriasRepository

def pagina_cadastro_categorias(caminho_banco: str):
    st.markdown("## 📂 Categorias de Saídas")

    repo = CategoriasRepository(caminho_banco)

    col_cat, col_sub = st.columns([1, 1])

    # ========== CATEGORIAS ==========
    with col_cat:
        st.markdown("### 🗃️ Categorias")
        with st.form("form_add_categoria"):
            nome_cat = st.text_input("Nova categoria", key="cat_nome")
            add_cat = st.form_submit_button("➕ Adicionar categoria")
        if add_cat:
            if not (nome_cat or "").strip():
                st.warning("Informe um nome.")
            else:
                repo.adicionar_categoria(nome_cat.strip())
                st.success("Categoria adicionada.")
                st.rerun()

        df_cat = repo.listar_categorias()
        if df_cat.empty:
            st.info("Nenhuma categoria cadastrada ainda.")
        else:
            for _, r in df_cat.iterrows():
                c1, c2 = st.columns([5, 1])
                c1.write(f"• {r['nome']}")
                if c2.button("🗑️", key=f"del_cat_{r['id']}"):
                    repo.excluir_categoria(int(r["id"]))
                    st.success("Categoria excluída.")
                    st.rerun()

    # ========== SUBCATEGORIAS ==========
    with col_sub:
        st.markdown("### 🧩 Subcategorias")
        df_cat2 = repo.listar_categorias()
        if df_cat2.empty:
            st.info("Cadastre uma categoria para começar a adicionar subcategorias.")
            return

        nomes = df_cat2["nome"].tolist()
        nome_escolhido = st.selectbox("Categoria", nomes, key="sub_sel_categoria")
        cat_row = df_cat2[df_cat2["nome"] == nome_escolhido].iloc[0]
        cat_id = int(cat_row["id"])

        with st.form("form_add_subcategoria"):
            nome_sub = st.text_input("Nova subcategoria", key="sub_nome")
            add_sub = st.form_submit_button("➕ Adicionar subcategoria")
        if add_sub:
            if not (nome_sub or "").strip():
                st.warning("Informe um nome.")
            else:
                repo.adicionar_subcategoria(cat_id, nome_sub.strip())
                st.success("Subcategoria adicionada.")
                st.rerun()

        df_sub = repo.listar_subcategorias(cat_id)
        if df_sub.empty:
            st.info("Nenhuma subcategoria nesta categoria.")
        else:
            for _, r in df_sub.iterrows():
                c1, c2 = st.columns([5, 1])
                c1.write(f"• {r['nome']}")
                if c2.button("🗑️", key=f"del_sub_{r['id']}"):
                    repo.excluir_subcategoria(int(r["id"]))
                    st.success("Subcategoria excluída.")
                    st.rerun()