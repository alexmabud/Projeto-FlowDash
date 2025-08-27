"""
Página: Cadastro de Saldos Bancários
====================================

Permite somar valores ao saldo de um banco específico na data escolhida, criando
ou atualizando a linha de `saldos_bancos` (uma linha por data) e registrando a
entrada correspondente em `movimentacoes_bancarias` com observação padronizada,
usuário e timestamp.

Comportamentos:
- Garante colunas dinâmicas para todos os bancos cadastrados (ALTER TABLE se faltar).
- Se já existir linha para a data, soma no campo do banco; senão cria a linha.
- Registra a movimentação bancária (entrada) com:
  - observação: "Cadastro REGISTRO MANUAL DE SALDO BANCÁRIO | Valor R$ X"
  - usuario: nome do usuário logado
  - data_hora: YYYY-MM-DD HH:MM:SS
"""

import re
import sqlite3
from datetime import date, datetime
import pandas as pd
import streamlit as st

from repository.movimentacoes_repository import MovimentacoesRepository
from flowdash_pages.cadastros.cadastro_classes import BancoRepository


# ------------------------- helpers internos -------------------------
def _garantir_colunas_bancos(conn: sqlite3.Connection, bancos: list[str]) -> None:
    """Garante colunas para todos os bancos na tabela `saldos_bancos` (DEFAULT 0.0)."""
    cols_info = conn.execute("PRAGMA table_info(saldos_bancos)").fetchall()
    existentes = {c[1] for c in cols_info}
    faltantes = [b for b in bancos if b not in existentes]
    for b in faltantes:
        conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{b}" REAL DEFAULT 0.0')


def _formatar_moeda_br(v: float) -> str:
    """Formata número como moeda BR: R$ 1.234,56 (sem depender de locale)."""
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


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
    """Obtém o nome do usuário logado via auth/session_state. Pede confirmação se necessário."""
    # 1) Tenta módulo auth (se existir)
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

    # 2) Chaves diretas do session_state
    diretas = [
        "nome_usuario", "usuario_nome", "user_nome", "user_name",
        "nome", "nomeCompleto", "usuario_atual_nome", "current_user_name",
        "display_name", "full_name",
    ]
    for k in diretas:
        v = st.session_state.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # 3) Objetos/dicts aninhados + varredura completa
    compostas = ["usuario", "user", "current_user", "auth_user", "logged_user", "auth"]
    for k in compostas:
        nm = _walk_extract_names(st.session_state.get(k))
        if nm:
            return nm
    for k in list(st.session_state.keys()):
        nm = _walk_extract_names(st.session_state.get(k))
        if nm:
            return nm

    # 4) Fallback por e-mail/login
    for k in ("usuario_email", "user_email", "email", "login", "username"):
        v = st.session_state.get(k)
        if isinstance(v, str) and v.strip():
            nm = _derive_nome_from_email(v)
            if nm:
                return nm

    # 5) Por fim, pede confirmação ao usuário
    usuario_atual = st.text_input(
        "Usuário logado",
        value=st.session_state.get("usuario_confirmado", ""),
        placeholder="Digite seu nome para registrar nos lançamentos bancários",
        help="Não consegui capturar seu nome automaticamente. Confirme aqui para gravar corretamente."
    ).strip()
    if usuario_atual:
        st.session_state["usuario_confirmado"] = usuario_atual
    return usuario_atual


def _inserir_mov_bancaria(
    caminho_banco: str,
    data_: str,
    banco: str,
    valor: float,
    *,
    referencia_id: int | None = None,
    usuario: str | None = None,
    data_hora: str | None = None,
) -> None:
    """
    Registra ENTRADA em `movimentacoes_bancarias` (valor > 0) com origem 'saldos_bancos',
    observação padronizada e metadados de usuário/timestamp, usando repositório idempotente.

    Args:
        caminho_banco: Caminho do SQLite.
        data_: Data no formato 'YYYY-MM-DD'.
        banco: Nome do banco (coluna em `saldos_bancos`).
        valor: Valor a registrar (deve ser > 0).
        referencia_id: rowid de `saldos_bancos` correspondente à data.
        usuario: Nome do usuário logado (se None, não grava).
        data_hora: Timestamp 'YYYY-MM-DD HH:MM:SS' (se None, não grava).
    """
    try:
        if valor is None or float(valor) <= 0:
            return
        mov_repo = MovimentacoesRepository(caminho_banco)
        observacao = f"Cadastro REGISTRO MANUAL DE SALDO BANCÁRIO | Valor {_formatar_moeda_br(valor)}"
        mov_repo.registrar_entrada(
            data=str(data_),
            banco=str(banco or ""),
            valor=float(valor),
            origem="saldos_bancos",
            observacao=observacao,
            referencia_tabela="saldos_bancos",
            referencia_id=referencia_id,
            usuario=usuario,
            data_hora=data_hora,
        )
    except Exception as e:
        st.warning(f"Não foi possível registrar movimentação para {banco}: {e}")


# ------------------------- página principal -------------------------
def pagina_saldos_bancarios(caminho_banco: str) -> None:
    """Renderiza a página de **Cadastro de Saldos Bancários por Banco** (soma por data)."""
    st.subheader("🏦 Cadastro de Saldos Bancários por Banco (soma na mesma data)")

    # Mensagem persistente pós-rerun
    if "mensagem_sucesso" in st.session_state:
        st.success(st.session_state.pop("mensagem_sucesso"))

    # Data do lançamento
    data_sel = st.date_input("📅 Data do lançamento", value=date.today())
    data_str = str(data_sel)

    # Bancos cadastrados
    repo_banco = BancoRepository(caminho_banco)
    df_bancos = repo_banco.carregar_bancos()

    if df_bancos.empty:
        st.warning("⚠️ Nenhum banco cadastrado. Cadastre um banco primeiro.")
        return

    bancos = df_bancos["nome"].tolist()
    banco_selecionado = st.selectbox("🏦 Banco", bancos)
    valor_digitado = st.number_input(
        "💰 Valor a somar no saldo do banco na data selecionada",
        min_value=0.0, step=10.0, format="%.2f"
    )

    # Captura usuário e timestamp
    usuario_atual = _resolver_usuario_logado()
    if not usuario_atual:
        st.warning("⚠️ Confirme o campo **Usuário logado** acima para continuar.")
    agora_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if st.button("💾 Lançar Saldo (somar na mesma data)", use_container_width=True, disabled=(not usuario_atual)):
        try:
            with sqlite3.connect(caminho_banco) as conn:
                cur = conn.cursor()

                # Garante as colunas para todos os bancos
                _garantir_colunas_bancos(conn, bancos)

                # Verifica se já existe linha para a data
                row = cur.execute(
                    "SELECT rowid FROM saldos_bancos WHERE data = ? LIMIT 1;",
                    (data_str,)
                ).fetchone()

                if row:
                    # UPDATE acumulando na coluna do banco
                    cur.execute(
                        f'UPDATE saldos_bancos '
                        f'SET "{banco_selecionado}" = COALESCE("{banco_selecionado}", 0) + ? '
                        f'WHERE data = ?;',
                        (float(valor_digitado), data_str)
                    )
                    # Para referência, buscamos o rowid atualizado
                    referencia_id = cur.execute(
                        "SELECT rowid FROM saldos_bancos WHERE data = ? LIMIT 1;",
                        (data_str,),
                    ).fetchone()
                    referencia_id = int(referencia_id[0]) if referencia_id else None
                else:
                    # INSERT de nova linha com a data; somente a coluna do banco recebe o valor
                    colunas = ["data"] + bancos
                    valores = [data_str] + [
                        float(valor_digitado) if b == banco_selecionado else 0.0
                        for b in bancos
                    ]
                    placeholders = ",".join(["?"] * len(colunas))
                    colunas_sql = ",".join([f'"{c}"' for c in colunas])

                    cur.execute(
                        f'INSERT INTO saldos_bancos ({colunas_sql}) VALUES ({placeholders})',
                        valores
                    )
                    referencia_id = int(cur.lastrowid)  # funciona mesmo sem coluna 'id'

                conn.commit()

            # também lança em movimentacoes_bancarias como ENTRADA, com referência e metadados
            _inserir_mov_bancaria(
                caminho_banco=caminho_banco,
                data_=data_str,
                banco=banco_selecionado,
                valor=valor_digitado,
                referencia_id=referencia_id,
                usuario=usuario_atual,
                data_hora=agora_str,
            )

            valor_fmt = _formatar_moeda_br(valor_digitado)
            st.session_state["mensagem_sucesso"] = (
                f"✅ Somado {valor_fmt} em **{banco_selecionado}** "
                f"na data {pd.to_datetime(data_str).strftime('%d/%m/%Y')}."
            )
            st.rerun()

        except Exception as e:
            st.error(f"❌ Erro ao lançar saldo: {e}")

    # --- Últimos lançamentos (append + updates por data) ---
    st.markdown("---")
    st.markdown("### 📋 Últimos Lançamentos (saldos_bancos)")

    try:
        with sqlite3.connect(caminho_banco) as conn:
            # ordena por id se existir; senão por data
            cols_info = conn.execute("PRAGMA table_info(saldos_bancos)").fetchall()
            cols_existentes = {c[1] for c in cols_info}
            order_sql = "ORDER BY id DESC" if "id" in cols_existentes else "ORDER BY data DESC"
            df_saldos = pd.read_sql(f"SELECT * FROM saldos_bancos {order_sql} LIMIT 30", conn)

        if not df_saldos.empty:
            if "data" in df_saldos.columns:
                df_saldos["data"] = pd.to_datetime(df_saldos["data"], errors="coerce").dt.strftime("%d/%m/%Y")

            for banco in bancos:
                if banco in df_saldos.columns:
                    df_saldos[banco] = df_saldos[banco].apply(
                        lambda x: _formatar_moeda_br(x) if pd.notnull(x) else ""
                    )

            if "data" in df_saldos.columns:
                df_saldos = df_saldos.rename(columns={"data": "Data"})

            st.dataframe(df_saldos, use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ Nenhum lançamento registrado ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar os lançamentos: {e}")
