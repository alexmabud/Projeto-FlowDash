# import streamlit as st
# import sqlite3
# import pandas as pd
# from datetime import date, datetime, timedelta

# from utils.utils import formatar_valor


# # =========================
# # Helpers gerais da página
# # =========================

# def get_conn(caminho_banco: str):
#     """Abre conexão SQLite com timeout maior para evitar SQLITE_BUSY."""
#     conn = sqlite3.connect(caminho_banco, timeout=30)
#     try:
#         conn.execute("PRAGMA journal_mode=WAL;")
#         conn.execute("PRAGMA synchronous=NORMAL;")
#     except Exception:
#         pass
#     return conn

# DIAS_COMPENSACAO = {
#     "DINHEIRO": 0,
#     "PIX": 0,
#     "DÉBITO": 1,
#     "CRÉDITO": 1,
#     "LINK_PAGAMENTO": 1,
# }

# def proximo_dia_util_br(data_base: date, dias: int) -> date:
#     """
#     Retorna a data de liquidação em 'dias' úteis à frente.
#     - Tenta usar feriados de Brasília (workalendar). Se não houver, considera apenas fins de semana.
#     """
#     try:
#         from workalendar.america import BrazilDistritoFederal
#         cal = BrazilDistritoFederal()
#         d = data_base
#         adicionados = 0
#         while adicionados < dias:
#             d += timedelta(days=1)
#             if cal.is_working_day(d):
#                 adicionados += 1
#         return d
#     except Exception:
#         # fallback: fins de semana apenas
#         d = data_base
#         adicionados = 0
#         while adicionados < dias:
#             d += timedelta(days=1)
#             if d.weekday() < 5:  # 0=seg ... 6=dom
#                 adicionados += 1
#         return d

# def carregar_tabela(nome_tabela, caminho_banco):
#     try:
#         with get_conn(caminho_banco) as conn:
#             df = pd.read_sql(f"SELECT * FROM {nome_tabela}", conn)
#             if "Data" in df.columns:
#                 df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.strftime("%Y-%m-%d")
#             return df
#     except Exception:
#         return pd.DataFrame()

# def bloco_resumo_dia(itens):
#     st.markdown(f"""
#     <div style='border: 1px solid #444; border-radius: 10px; padding: 20px; background-color: #1c1c1c; margin-bottom: 20px;'>
#         <h4 style='color: white;'>📆 Resumo Financeiro de Hoje</h4>
#         <table style='width: 100%; margin-top: 15px;'>
#             <tr>
#                 {''.join([
#                     f"<td style='text-align: center; width: 33%;'>"
#                     f"<div style='color: #ccc; font-weight: bold;'>{label}</div>"
#                     f"<div style='font-size: 1.6rem; color: #00FFAA;'>{valor}</div>"
#                     f"</td>"
#                     for label, valor in itens
#                 ])}
#             </tr>
#         </table>
#     </div>
#     """, unsafe_allow_html=True)

# def inserir_mov_liquidacao_venda(caminho_banco: str, data_: str, banco: str, valor_liquido: float,
#                                  observacao: str, referencia_id: int | None):
#     if not valor_liquido or valor_liquido <= 0:
#         return
#     with get_conn(caminho_banco) as conn:
#         conn.execute("""
#             INSERT INTO movimentacoes_bancarias (data, banco, tipo, valor, origem, observacao, referencia_id)
#             VALUES (?, ?, 'entrada', ?, 'vendas', ?, ?)
#         """, (data_, banco, float(valor_liquido), observacao, referencia_id))
#         conn.commit()

# # ===== Novo: helpers de Saída ===============================================================
# def inserir_mov_saida(caminho_banco: str, data_: str, banco: str, valor: float,
#                       observacao: str, referencia_id: int | None, conn: sqlite3.Connection | None = None):
#     if not valor or valor <= 0:
#         return
#     own_conn = False
#     if conn is None:
#         conn = get_conn(caminho_banco)
#         own_conn = True
#     conn.execute(
#         """
#         INSERT INTO movimentacoes_bancarias (data, banco, tipo, valor, origem, observacao, referencia_id)
#         VALUES (?, ?, 'saida', ?, 'saidas', ?, ?)
#         """,
#         (data_, banco, float(valor), observacao, referencia_id)
#     )
#     if own_conn:
#         conn.commit()
#         conn.close()


# def salvar_saida_registro(caminho_banco: str, data_: str, valor: float,
#                           categoria: str | None, origem: str | None, usuario: str | None) -> int:
#     """Insere na tabela 'saida' usando apenas as colunas existentes (auto-detecta via PRAGMA).
#     Retorna o id gerado.
#     """
#     with get_conn(caminho_banco) as conn:
#         cur = conn.cursor()
#         cols = [r[1] for r in cur.execute("PRAGMA table_info(saida)").fetchall()]

#         # mapeia nomes possíveis
#         data_col  = 'Data'  if 'Data'  in cols else ('data'  if 'data'  in cols else None)
#         valor_col = 'Valor' if 'Valor' in cols else ('valor' if 'valor' in cols else None)
#         if not data_col or not valor_col:
#             raise RuntimeError("Tabela 'saida' precisa ter as colunas Data/Valor.")

#         payload = {data_col: data_, valor_col: float(valor)}
#         # opcionais
#         if 'Categoria' in cols and categoria is not None: payload['Categoria'] = categoria
#         if 'categoria' in cols and categoria is not None: payload['categoria'] = categoria
#         if 'Origem' in cols and origem is not None:       payload['Origem'] = origem
#         if 'origem' in cols and origem is not None:       payload['origem'] = origem
#         if 'Usuario' in cols and usuario is not None:     payload['Usuario'] = usuario
#         if 'usuario' in cols and usuario is not None:     payload['usuario'] = usuario
#         if 'created_at' in cols:                          payload['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#         cols_sql = ", ".join(payload.keys())
#         qmarks   = ", ".join(["?"] * len(payload))
#         cur.execute(f"INSERT INTO saida ({cols_sql}) VALUES ({qmarks})", tuple(payload.values()))
#         conn.commit()
#         return cur.lastrowid

# def registrar_caixa_vendas(caminho_banco: str, data_: str, valor: float):
#     """
#     Atualiza a tabela existente saldos_caixas acumulando em caixa_vendas na 'data_'.
#     - Não cria tabela nova.
#     - Tenta primeiro com coluna 'data'; se não existir, tenta 'Data'.
#     """
#     if not valor or valor <= 0:
#         return
#     with get_conn(caminho_banco) as conn:
#         cur = conn.cursor()
#         try:
#             # esquema com coluna 'data'
#             cur.execute(
#                 "UPDATE saldos_caixas SET caixa_vendas = COALESCE(caixa_vendas,0) + ? WHERE data = ?",
#                 (float(valor), data_)
#             )
#             if cur.rowcount == 0:
#                 cur.execute(
#                     "INSERT INTO saldos_caixas (data, caixa_vendas) VALUES (?, ?)",
#                     (data_, float(valor))
#                 )
#         except sqlite3.OperationalError:
#             # fallback para esquema com coluna 'Data'
#             cur.execute(
#                 "UPDATE saldos_caixas SET caixa_vendas = COALESCE(caixa_vendas,0) + ? WHERE Data = ?",
#                 (float(valor), data_)
#             )
#             if cur.rowcount == 0:
#                 cur.execute(
#                     "INSERT INTO saldos_caixas (Data, caixa_vendas) VALUES (?, ?)",
#                     (data_, float(valor))
#                 )
#         conn.commit()

# def obter_banco_destino(caminho_banco: str, forma: str, maquineta: str, bandeira: str | None, parcelas: int | None) -> str | None:
#     """
#     Descobre o banco_destino lendo a tabela taxas_maquinas.
#     Estratégia:
#       1) Match exato (forma, maquineta, bandeira, parcelas)
#       2) Se forma == LINK_PAGAMENTO, tenta como CRÉDITO também
#       3) Match por (forma, maquineta) ignorando bandeira/parcelas
#       4) Qualquer registro da maquineta
#     """
#     formas_try = [forma]
#     if forma == "LINK_PAGAMENTO":
#         formas_try.append("CRÉDITO")

#     with get_conn(caminho_banco) as conn:
#         for f in formas_try:
#             row = conn.execute("""
#                 SELECT banco_destino
#                 FROM taxas_maquinas
#                 WHERE forma_pagamento = ?
#                   AND maquineta       = ?
#                   AND bandeira        = ?
#                   AND parcelas        = ?
#                 LIMIT 1
#             """, (f, maquineta or "", bandeira or "", int(parcelas or 1))).fetchone()
#             if row and row[0]:
#                 return row[0]

#         for f in formas_try:
#             row = conn.execute("""
#                 SELECT banco_destino
#                 FROM taxas_maquinas
#                 WHERE forma_pagamento = ?
#                   AND maquineta       = ?
#                   AND banco_destino IS NOT NULL
#                   AND TRIM(banco_destino) <> ''
#                 LIMIT 1
#             """, (f, maquineta or "")).fetchone()
#             if row and row[0]:
#                 return row[0]

#         row = conn.execute("""
#             SELECT banco_destino
#             FROM taxas_maquinas
#             WHERE maquineta = ?
#               AND banco_destino IS NOT NULL
#               AND TRIM(banco_destino) <> ''
#             LIMIT 1
#         """, (maquineta or "",)).fetchone()
#         if row and row[0]:
#             return row[0]

#     return None


# # =========================
# # Página principal
# # =========================

# def pagina_lancamentos(caminho_banco):
#     # Mensagem persistente de sucesso
#     if "msg_ok" in st.session_state:
#         st.success(st.session_state["msg_ok"])
#         del st.session_state["msg_ok"]

#     # Data do lançamento
#     data_lancamento = st.date_input("🗓️ Selecione a Data do Lançamento", value=date.today(), key="data_lancamento")
#     st.markdown(
#         f"## 🧾 Lançamentos do Dia — <span style='color:#00FFAA'><b>{data_lancamento}</b></span>",
#         unsafe_allow_html=True
#     )
#     data_str = str(data_lancamento)

#     # === Resumo do dia ===
#     st.markdown("### 📊 Resumo do Dia")
#     df_entrada = carregar_tabela("entrada", caminho_banco)
#     df_saida = carregar_tabela("saida", caminho_banco)
#     df_mercadorias = carregar_tabela("mercadorias", caminho_banco)

#     total_entrada = df_entrada[df_entrada["Data"] == data_str]["Valor"].sum() if "Valor" in df_entrada.columns else 0.0
#     total_saida = df_saida[df_saida["Data"] == data_str]["Valor"].sum() if "Valor" in df_saida.columns else 0.0
#     total_mercadorias = df_mercadorias[df_mercadorias["Data"] == data_str]["Valor_Mercadoria"].sum() if "Valor_Mercadoria" in df_mercadorias.columns else 0.0

#     bloco_resumo_dia([
#         ("Entradas", formatar_valor(total_entrada)),
#         ("Saídas", formatar_valor(total_saida)),
#         ("Mercadorias", formatar_valor(total_mercadorias))
#     ])

#     # === Ações ===
#     st.markdown("### ➕ Ações do Dia")
#     col1, col2 = st.columns(2)

#     # -----------------------------
#     # Coluna 1 — Nova Venda / Caixa 2
#     # -----------------------------
#     with col1:
#         if st.button("🟢 Nova Venda", use_container_width=True):
#             st.session_state.form_venda = not st.session_state.get("form_venda", False)

#         if st.session_state.get("form_venda", False):
#             st.markdown("#### 📋 Nova Venda")

#             valor = st.number_input("Valor da Venda", min_value=0.0, step=0.01, key="valor_venda")
#             forma_pagamento = st.selectbox(
#                 "Forma de Pagamento",
#                 ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"],
#                 key="forma_pagamento_venda"
#             )

#             parcelas = 1
#             bandeira = ""
#             maquineta = ""
#             banco_pix_direto = None
#             taxa_pix_direto = 0.0

#             # Carrega maquinetas existentes na taxa
#             try:
#                 with get_conn(caminho_banco) as conn:
#                     maquinetas_all = pd.read_sql("SELECT DISTINCT maquineta FROM taxas_maquinas ORDER BY maquineta", conn)["maquineta"].tolist()
#             except Exception:
#                 maquinetas_all = []

#             if forma_pagamento == "PIX":
#                 modo_pix = st.radio(
#                     "Como será o PIX?",
#                     ["Via maquineta", "Direto para banco"],
#                     horizontal=True,
#                     key="modo_pix"
#                 )
#                 if modo_pix == "Via maquineta":
#                     # maquineta obrigatória para pix via maquineta
#                     try:
#                         with get_conn(caminho_banco) as conn:
#                             maq_pix = pd.read_sql(
#                                 """
#                                 SELECT DISTINCT maquineta
#                                 FROM taxas_maquinas
#                                 WHERE forma_pagamento = 'PIX'
#                                 ORDER BY maquineta
#                                 """,
#                                 conn
#                             )["maquineta"].tolist()
#                     except Exception:
#                         maq_pix = []
#                     maquineta = st.selectbox("PSP/Maquineta do PIX", maq_pix, key="maquineta_pix")
#                     bandeira = ""
#                     parcelas = 1
#                 else:
#                     # PIX direto para banco
#                     maquineta = ""
#                     bandeira = ""
#                     parcelas = 1
#                     try:
#                         with get_conn(caminho_banco) as conn:
#                             df_bancos = pd.read_sql("SELECT nome FROM bancos_cadastrados ORDER BY nome", conn)
#                         bancos_lista = df_bancos["nome"].tolist()
#                     except Exception:
#                         bancos_lista = []
#                     banco_pix_direto = st.selectbox("Banco que receberá o PIX", bancos_lista, key="banco_pix_direto")
#                     taxa_pix_direto = st.number_input("Taxa do PIX direto (%)", min_value=0.0, step=0.01, format="%.2f", value=0.0, key="taxa_pix_direto")

#             elif forma_pagamento in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"]:
#                 maquineta = st.selectbox("Maquineta", maquinetas_all, key="maquineta_cartao")
#                 # Bandeira
#                 try:
#                     with get_conn(caminho_banco) as conn:
#                         bandeiras = pd.read_sql(
#                             """
#                             SELECT DISTINCT bandeira FROM taxas_maquinas
#                             WHERE forma_pagamento = ? AND maquineta = ?
#                             ORDER BY bandeira
#                             """,
#                             conn,
#                             params=(forma_pagamento if forma_pagamento != "LINK_PAGAMENTO" else "CRÉDITO", maquineta)
#                         )["bandeira"].tolist()
#                 except Exception:
#                     bandeiras = []
#                 bandeira = st.selectbox("Bandeira", bandeiras, key="bandeira_cartao") if bandeiras else ""

#                 # Parcelas
#                 if forma_pagamento in ["CRÉDITO", "LINK_PAGAMENTO"]:
#                     try:
#                         with get_conn(caminho_banco) as conn:
#                             parcelas_disp = pd.read_sql(
#                                 """
#                                 SELECT DISTINCT parcelas FROM taxas_maquinas
#                                 WHERE forma_pagamento = ? AND maquineta = ? AND bandeira = ?
#                                 ORDER BY parcelas
#                                 """,
#                                 conn,
#                                 params=(forma_pagamento if forma_pagamento != "LINK_PAGAMENTO" else "CRÉDITO", maquineta, bandeira)
#                             )["parcelas"].tolist()
#                     except Exception:
#                         parcelas_disp = []
#                     parcelas = st.selectbox("Parcelas", parcelas_disp if parcelas_disp else [1], key="parcelas_cartao")
#                 else:
#                     parcelas = 1

#             elif forma_pagamento == "DINHEIRO":
#                 # Venda em dinheiro sempre vai para o Caixa, sem maquineta e sem banco
#                 maquineta = ""
#                 bandeira = ""
#                 parcelas = 1
#                 st.caption("🧾 Venda em dinheiro será registrada no **Caixa** e também no livro de movimentações.")

#             # Resumo antes de salvar
#             confirmar = st.checkbox("Confirmo os dados para salvar a venda", key="confirmar_venda")

#             if confirmar:
#                 resumo = [
#                     f"- Valor: R$ {valor:,.2f}",
#                     f"- Forma de pagamento: {forma_pagamento}",
#                 ]
#                 if maquineta:
#                     resumo.append(f"- Maquineta: {maquineta}")
#                 if bandeira:
#                     resumo.append(f"- Bandeira: {bandeira}")
#                 if forma_pagamento in ["CRÉDITO", "LINK_PAGAMENTO"]:
#                     resumo.append(f"- Parcelas: {parcelas}")
#                 if forma_pagamento == "PIX" and st.session_state.get("modo_pix") == "Direto para banco" and banco_pix_direto:
#                     resumo.append(f"- Banco PIX direto: {banco_pix_direto} (taxa {taxa_pix_direto:.2f}%)")

#                 st.info("**Resumo da Venda:**\n\n" + "\n".join(resumo))

#             # ===== Salvar Venda =====
#             if st.button("💾 Salvar Venda", use_container_width=True):
#                 # validações básicas
#                 if valor <= 0:
#                     st.warning("⚠️ Valor inválido.")
#                     st.stop()
#                 if not confirmar:
#                     st.warning("⚠️ Confirme os dados antes de salvar.")
#                     st.stop()
#                 if forma_pagamento in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"] and (not maquineta or not bandeira):
#                     st.warning("⚠️ Selecione maquineta e bandeira.")
#                     st.stop()
#                 if forma_pagamento == "PIX" and st.session_state.get("modo_pix") == "Via maquineta" and not maquineta:
#                     st.warning("⚠️ Selecione a maquineta do PIX.")
#                     st.stop()

#                 try:
#                     # 1) calcular taxa e banco_destino
#                     taxa = 0.0
#                     banco_destino = None

#                     if forma_pagamento in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"]:
#                         # taxa e banco pela taxa cadastrada
#                         with get_conn(caminho_banco) as conn:
#                             row = conn.execute(
#                                 """
#                                 SELECT taxa_percentual, banco_destino
#                                 FROM taxas_maquinas
#                                 WHERE forma_pagamento = ?
#                                   AND maquineta       = ?
#                                   AND bandeira        = ?
#                                   AND parcelas        = ?
#                                 LIMIT 1
#                                 """,
#                                 (
#                                     forma_pagamento if forma_pagamento != "LINK_PAGAMENTO" else "CRÉDITO",
#                                     maquineta, bandeira, int(parcelas or 1)
#                                 )
#                             ).fetchone()
#                         if row:
#                             taxa = float(row[0] or 0.0)
#                             banco_destino = row[1] if row[1] else None
#                         # fallback
#                         if not banco_destino:
#                             banco_destino = obter_banco_destino(caminho_banco, forma_pagamento, maquineta, bandeira, parcelas)

#                     elif forma_pagamento == "PIX":
#                         if st.session_state.get("modo_pix") == "Via maquineta":
#                             with get_conn(caminho_banco) as conn:
#                                 row = conn.execute(
#                                     """
#                                     SELECT taxa_percentual, banco_destino
#                                     FROM taxas_maquinas
#                                     WHERE forma_pagamento = 'PIX'
#                                       AND maquineta       = ?
#                                       AND bandeira        = ''
#                                       AND parcelas        = 1
#                                     LIMIT 1
#                                     """,
#                                     (maquineta,)
#                                 ).fetchone()
#                             taxa = float(row[0] or 0.0) if row else 0.0
#                             banco_destino = (row[1] if row and row[1] else None)
#                             if not banco_destino:
#                                 banco_destino = obter_banco_destino(caminho_banco, "PIX", maquineta, "", 1)
#                         else:
#                             # PIX direto para banco
#                             banco_destino = banco_pix_direto
#                             taxa = float(taxa_pix_direto or 0.0)
#                             if not banco_destino:
#                                 st.warning("⚠️ Selecione o banco que receberá o PIX direto.")
#                                 st.stop()

#                     elif forma_pagamento == "DINHEIRO":
#                         # Sem taxa; registra como entrada no "Caixa"
#                         taxa = 0.0
#                         banco_destino = "Caixa"   # padrão para dinheiro
#                         parcelas = 1
#                         bandeira = ""
#                         maquineta = ""

#                     # 2) calcula valor líquido
#                     valor_liquido = float(valor) * (1 - float(taxa) / 100.0)

#                     # 3) grava na ENTRADA (com valor_liquido)
#                     with get_conn(caminho_banco) as conn:
#                         usuario = st.session_state.usuario_logado["nome"] if "usuario_logado" in st.session_state and st.session_state.usuario_logado else "Sistema"
#                         cur = conn.execute(
#                             """
#                             INSERT INTO entrada (Data, Valor, Forma_de_Pagamento, Parcelas, Bandeira, Usuario, maquineta, valor_liquido, created_at)
#                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
#                             """,
#                             (
#                                 str(data_lancamento),
#                                 float(valor),
#                                 forma_pagamento,
#                                 int(parcelas or 1),
#                                 bandeira,
#                                 usuario,
#                                 maquineta,
#                                 round(valor_liquido, 2),
#                                 datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                             )
#                         )
#                         venda_id = cur.lastrowid
#                         conn.commit()

#                     # 4) agenda/insere liquidação no livro de movimentações (inclui DINHEIRO)
#                     data_base = pd.to_datetime(data_lancamento).date()
#                     dias = DIAS_COMPENSACAO.get(forma_pagamento, 0)
#                     data_liq = proximo_dia_util_br(data_base, dias) if dias > 0 else data_base

#                     obs = (
#                         f"Liquidação {forma_pagamento} {maquineta or ''}{('/' + bandeira) if bandeira else ''} {int(parcelas or 1)}x"
#                         if forma_pagamento != "DINHEIRO"
#                         else "Venda DINHEIRO - Caixa"
#                     ).strip()

#                     if not banco_destino:
#                         st.warning("⚠️ Não foi possível identificar o banco de destino. A movimentação NÃO foi lançada.")
#                     else:
#                         inserir_mov_liquidacao_venda(
#                             caminho_banco=caminho_banco,
#                             data_=str(data_liq),
#                             banco=banco_destino,
#                             valor_liquido=round(valor_liquido, 2),
#                             observacao=obs,
#                             referencia_id=venda_id
#                         )

#                     # ⬇️ Para DINHEIRO, acumula também em saldos_caixa.caixa_vendas
#                     if forma_pagamento == "DINHEIRO":
#                         registrar_caixa_vendas(caminho_banco, str(data_liq), float(valor))

#                     # Mensagem persistente
#                     if forma_pagamento == "DINHEIRO":
#                         st.session_state["msg_ok"] = (
#                             f"✅ Venda registrada! **{formatar_valor(valor)}** no **Caixa** "
#                             f"({data_liq.strftime('%d/%m/%Y')}) e lançada em movimentações e saldos do caixa."
#                         )
#                     else:
#                         st.session_state["msg_ok"] = (
#                             f"✅ Venda registrada! Liquidação de **{formatar_valor(valor_liquido)}** "
#                             f"em **{banco_destino or '—'}** na data **{data_liq.strftime('%d/%m/%Y')}**."
#                         )

#                     # Debug rápido das últimas liquidações
#                     try:
#                         with get_conn(caminho_banco) as conn:
#                             df_dbg = pd.read_sql(
#                                 """
#                                 SELECT id, data, banco, tipo, valor, origem, observacao, referencia_id
#                                 FROM movimentacoes_bancarias
#                                 WHERE origem = 'vendas'
#                                 ORDER BY id DESC
#                                 LIMIT 10
#                                 """,
#                                 conn
#                             )
#                         if not df_dbg.empty:
#                             st.caption("🔎 Últimos lançamentos de liquidação (origem = vendas):")
#                             st.dataframe(df_dbg, use_container_width=True, hide_index=True)
#                     except Exception as e:
#                         st.warning(f"Não consegui listar as movimentações: {e}")

#                     st.session_state.form_venda = False
#                     st.rerun()

#                 except Exception as e:
#                     st.error(f"Erro ao salvar venda: {e}")

#         # Caixa 2 (placeholder)
#         with st.container():
#             if st.button("🔄 Caixa 2", use_container_width=True):
#                 st.session_state.form_caixa2 = not st.session_state.get("form_caixa2", False)
#             if st.session_state.get("form_caixa2", False):
#                 st.markdown("#### 💸 Transferência para Caixa 2")
#                 st.number_input("Valor a Transferir", min_value=0.0, step=0.01, key="valor_caixa2")
#                 st.button("💾 Confirmar Transferência", use_container_width=True)

#     # -----------------------------
#     # Coluna 2 — Saída / Depósito
#     # -----------------------------
#     with col2:
#         with st.container():
#             if st.button("🔴 Saída", use_container_width=True):
#                 st.session_state.form_saida = not st.session_state.get("form_saida", False)
#             if st.session_state.get("form_saida", False):
#                 st.markdown("#### 📤 Lançar Saída")
#                 valor_saida = st.number_input("Valor da Saída", min_value=0.0, step=0.01, key="valor_saida")
#                 forma_pagamento_saida = st.selectbox("Forma de Pagamento", ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO"], key="forma_pagamento_saida")

#                 parcelas_saida = 1
#                 cartao_credito = ""
#                 banco_saida = ""
#                 origem_dinheiro = ""

#                 if forma_pagamento_saida == "CRÉDITO":
#                     parcelas_saida = st.selectbox("Parcelas", list(range(1, 13)), key="parcelas_saida")
#                     try:
#                         with get_conn(caminho_banco) as conn:
#                             df_cartoes = pd.read_sql("SELECT * FROM cartoes_credito", conn)
#                     except Exception:
#                         df_cartoes = pd.DataFrame()
#                     if not df_cartoes.empty:
#                         coluna_cartao = "nome" if "nome" in df_cartoes.columns else df_cartoes.columns[0]
#                         cartao_credito = st.selectbox("Cartão de Crédito", df_cartoes[coluna_cartao].tolist(), key="cartao_credito")
#                     else:
#                         st.warning("⚠️ Nenhum cartão de crédito cadastrado.")
#                         st.stop()

#                 elif forma_pagamento_saida == "DINHEIRO":
#                     origem_dinheiro = st.selectbox("Origem do Dinheiro", ["Caixa", "Caixa 2"], key="origem_dinheiro")

#                 elif forma_pagamento_saida in ["PIX", "DÉBITO"]:
#                     nomes_bancos = {
#                         "Banco 1": "Inter",
#                         "Banco 2": "Bradesco",
#                         "Banco 3": "InfinitePay",
#                         "Banco 4": "Outros Bancos"
#                     }
#                     nomes_visuais = list(nomes_bancos.values())
#                     banco_visual = st.selectbox("Banco da Saída", nomes_visuais, key="banco_saida")
#                     banco_saida = next((b for b, nome in nomes_bancos.items() if nome == banco_visual), "")

#                 categoria = st.selectbox("Categoria", ["Contas Fixas", "Contas"], key="categoria_saida")
#                 subcategorias_dict = {
#                     "Contas Fixas": [
#                         "Água", "Luz", "Contabilidade", "Presence", "Crédito Celular", "Microsoft 365",
#                         "Chat GPT", "Simples Nacional", "Consignado", "FGI Bradesco", "Pro Labore", "DARF Pro Labore",
#                         "Comissão", "Salário", "FGTS", "Vale Transporte", "Fundo de Promoção", "Aluguel Maquineta"
#                     ],
#                     "Contas": [
#                         "Manutenção/Limpeza", "Marketing", "Pgto Cartão de Crédito", "Outros"
#                     ]
#                 }
#                 subcategoria = st.selectbox("Subcategoria", subcategorias_dict.get(categoria, []), key="subcategoria_saida")
#                 descricao = st.text_input("Descrição (opcional)", key="descricao_saida")

#                 resumo_saida = f"Valor: R$ {valor_saida:.2f}, Forma: {forma_pagamento_saida}, Categoria: {categoria}, Subcategoria: {subcategoria}, Descrição: {descricao if descricao else 'N/A'}"
#                 st.info(f"✅ Confirme os dados da saída: → {resumo_saida}")
#                 confirmar_saida = st.checkbox("Está tudo certo com os dados acima?", key="confirmar_saida")

#                 if st.button("💾 Salvar Saída", use_container_width=True):
#                     if valor_saida <= 0:
#                         st.warning("⚠️ O valor deve ser maior que zero.")
#                         st.stop()
#                     if not confirmar_saida:
#                         st.warning("⚠️ Confirme os dados antes de salvar.")
#                         st.stop()

#                     try:
#                         with get_conn(caminho_banco) as conn:
#                             usuario = st.session_state.usuario_logado["nome"] if "usuario_logado" in st.session_state and st.session_state.usuario_logado else "Sistema"

#                             if forma_pagamento_saida == "CRÉDITO":
#                                 # Recarrega info do cartão pelo nome selecionado
#                                 df_cartoes2 = pd.read_sql("SELECT * FROM cartoes_credito", conn)
#                                 if df_cartoes2.empty:
#                                     raise RuntimeError("Nenhum cartão cadastrado.")
#                                 coluna_cartao2 = "nome" if "nome" in df_cartoes2.columns else df_cartoes2.columns[0]
#                                 df_cartao = df_cartoes2[df_cartoes2[coluna_cartao2] == cartao_credito].iloc[0]
#                                 fechamento = int(df_cartao["fechamento"]) if "fechamento" in df_cartao else 0
#                                 vencimento = int(df_cartao["vencimento"]) if "vencimento" in df_cartao else 1

#                                 data_compra = pd.to_datetime(data_lancamento)
#                                 dia_compra = data_compra.day
#                                 data_parcela_ini = data_compra + pd.DateOffset(months=1) if dia_compra > fechamento else data_compra
#                                 valor_parcela = round(float(valor_saida) / int(parcelas_saida), 2)

#                                 for parcela in range(1, int(parcelas_saida) + 1):
#                                     vencimento_parcela = data_parcela_ini.replace(day=vencimento) + pd.DateOffset(months=parcela - 1)
#                                     conn.execute(
#                                         """
#                                         INSERT INTO fatura_cartao (data, vencimento, cartao, parcela, total_parcelas, valor, categoria, sub_categoria, descricao, usuario)
#                                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#                                         """,
#                                         (
#                                             str(data_compra.date()),
#                                             str(vencimento_parcela.date()),
#                                             cartao_credito,
#                                             parcela,
#                                             int(parcelas_saida),
#                                             valor_parcela,
#                                             categoria,
#                                             subcategoria,
#                                             descricao,
#                                             usuario
#                                         )
#                                     )

#                             else:
#                                 # Insere em 'saida' conforme seu schema anterior
#                                 conn.execute(
#                                     """
#                                     INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao, Forma_de_Pagamento, Parcelas, Valor, Usuario, Origem_Dinheiro, Banco_Saida)
#                                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#                                     """,
#                                     (
#                                         str(data_lancamento),
#                                         categoria,
#                                         subcategoria,
#                                         descricao,
#                                         forma_pagamento_saida.upper(),
#                                         int(parcelas_saida),
#                                         float(valor_saida),
#                                         usuario,
#                                         origem_dinheiro,
#                                         banco_saida
#                                     )
#                                 )

#                                 # Se for DINHEIRO, baixa do saldo do caixa/cAIXA 2
#                                 if forma_pagamento_saida.upper() == "DINHEIRO":
#                                     campo = "caixa_total" if origem_dinheiro == "Caixa" else "caixa2_total"
#                                     try:
#                                         conn.execute(
#                                             f"""
#                                             UPDATE saldos_caixas
#                                             SET {campo} = COALESCE({campo}, 0) - ?
#                                             WHERE data = ?
#                                             """,
#                                             (float(valor_saida), str(data_lancamento))
#                                         )
#                                     except sqlite3.OperationalError:
#                                         conn.execute(
#                                             f"""
#                                             UPDATE saldos_caixas
#                                             SET {campo} = COALESCE({campo}, 0) - ?
#                                             WHERE Data = ?
#                                             """,
#                                             (float(valor_saida), str(data_lancamento))
#                                         )

#                                 # Registrar no livro de movimentações (PIX/DÉBITO/DINHEIRO)
#                                 if forma_pagamento_saida.upper() in ["DINHEIRO", "PIX", "DÉBITO"]:
#                                     banco_mov = origem_dinheiro if forma_pagamento_saida.upper() == "DINHEIRO" else (banco_visual if 'banco_visual' in locals() and banco_visual else banco_saida)
#                                     obs_mov = f"Saída {categoria}/{subcategoria}" + (f" - {descricao}" if descricao else "")
#                                     inserir_mov_saida(
#                                         caminho_banco=caminho_banco,
#                                         data_=str(data_lancamento),
#                                         banco=banco_mov,
#                                         valor=float(valor_saida),
#                                         observacao=obs_mov,
#                                         referencia_id=None,
#                                         conn=conn,
#                                     )

#                         conn.commit()
#                         st.session_state["msg_ok"] = f"✅ Saída registrada com sucesso! → {resumo_saida}"
#                         st.session_state.form_saida = False
#                         st.rerun()

#                     except Exception as e:
#                         st.error(f"Erro ao salvar saída: {e}")

#         with st.container():
#             if st.button("🏦 Depósito Bancário", use_container_width=True):
#                 st.session_state.form_deposito = not st.session_state.get("form_deposito", False)
#             if st.session_state.get("form_deposito", False):
#                 st.markdown("#### 🏦 Registrar Depósito Bancário")
#                 st.number_input("Valor Depositado", min_value=0.0, step=0.01, key="valor_deposito")
#                 st.selectbox("Banco Destino", ["Banco 1", "Banco 2", "Banco 3", "Banco 4"], key="banco_destino")
#                 st.button("💾 Salvar Depósito", use_container_width=True)

#     # Linha separada para Mercadorias (placeholder)
#     st.markdown("---")
#     with st.container():
#         if st.button("📦 Mercadorias", use_container_width=True):
#             st.session_state.form_mercadoria = not st.session_state.get("form_mercadoria", False)
#         if st.session_state.get("form_mercadoria", False):
#             st.markdown("#### 📦 Registro de Mercadorias")
#             st.text_input("Fornecedor", key="fornecedor")
#             st.number_input("Valor da Mercadoria", min_value=0.0, step=0.01, key="valor_mercadoria")
#             st.button("💾 Salvar Mercadoria", use_container_width=True)
