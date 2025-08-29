# ===================== Actions: Saída =====================
"""
Executa a MESMA lógica do módulo original de Saída (sem Streamlit aqui):
- Fluxos padrão: DINHEIRO, PIX/DÉBITO, CRÉDITO, BOLETO.
- Fluxos Pagamentos: Fatura Cartão, Boletos (parcela), Empréstimos (parcela).
- Canonicalização de banco preservada.

Validações que no original exibiam st.warning/st.error aqui geram ValueError/RuntimeError.
A página captura e exibe as mensagens.

ATUALIZAÇÃO:
- Para Boletos/Empréstimos a lista mostra o "valor em aberto" calculado como:
    em_aberto = valor_evento - valor_pago_acumulado
  Usando como base a coluna `valor_evento` (se existir). Caso não exista, cai para
  colunas conhecidas de valor (saldo, valor_a_pagar, etc).
"""

from __future__ import annotations

from typing import TypedDict, Optional, Callable, Tuple
from datetime import date
import pandas as pd

from shared.db import get_conn
from services.ledger import LedgerService
from repository.cartoes_repository import CartoesRepository, listar_destinos_fatura_em_aberto
from repository.categorias_repository import CategoriasRepository
from flowdash_pages.cadastros.cadastro_classes import BancoRepository
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository  # compat
from flowdash_pages.lancamentos.shared_ui import canonicalizar_banco  # usado no original

# ---------- Constantes (iguais ao original)
FORMAS = ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "BOLETO"]
ORIGENS_DINHEIRO = ["Caixa", "Caixa 2"]


# ---------------- Utils
def _distinct_lower_trim(series: pd.Series) -> list[str]:
    if series is None or series.empty:
        return []
    df = pd.DataFrame({"orig": series.fillna("").astype(str).str.strip()})
    df["key"] = df["orig"].str.lower().str.strip()
    df = df[df["key"] != ""].drop_duplicates("key", keep="first")
    return df["orig"].sort_values().tolist()


def _opcoes_pagamentos(caminho_banco: str, tipo: str) -> list[str]:
    """
    Compat LEGADA (não usada nos novos fluxos de Pagamentos).
    """
    with get_conn(caminho_banco) as conn:
        if tipo == "Fatura Cartão de Crédito":
            return []

        elif tipo == "Empréstimos e Financiamentos":
            df_emp = pd.read_sql(
                """
                SELECT DISTINCT
                    TRIM(
                        COALESCE(
                            NULLIF(TRIM(banco),''), NULLIF(TRIM(descricao),''), NULLIF(TRIM(tipo),'')
                        )
                    ) AS rotulo
                FROM emprestimos_financiamentos
                """,
                conn,
            )
            df_emp = df_emp.dropna()
            df_emp = df_emp[df_emp["rotulo"] != ""]
            return _distinct_lower_trim(df_emp["rotulo"]) if not df_emp.empty else []

        elif tipo == "Boletos":
            # evita confundir com cartões/emp.
            df_cart = pd.read_sql(
                "SELECT DISTINCT TRIM(nome) AS nome FROM cartoes_credito "
                "WHERE nome IS NOT NULL AND TRIM(nome) <> ''",
                conn,
            )
            cart_set = set(x.strip().lower() for x in (df_cart["nome"].dropna().tolist() if not df_cart.empty else []))

            df_emp = pd.read_sql(
                "SELECT DISTINCT TRIM(COALESCE(NULLIF(TRIM(banco),''),NULLIF(TRIM(descricao),''),NULLIF(TRIM(tipo),''))) AS rotulo "
                "FROM emprestimos_financiamentos",
                conn,
            )
            emp_set = set(x.strip().lower() for x in (df_emp["rotulo"].dropna().tolist() if not df_emp.empty else []))

            df_cred = pd.read_sql(
                """
                SELECT DISTINCT TRIM(credor) AS credor
                  FROM contas_a_pagar_mov
                 WHERE credor IS NOT NULL AND TRIM(credor) <> ''
                   AND UPPER(COALESCE(status,'EM ABERTO')) IN ('EM ABERTO','PARCIAL')
                 ORDER BY credor
                """,
                conn,
            )

            def eh_boleto_nome(nm: str) -> bool:
                lx = (nm or "").strip().lower()
                return bool(lx) and (lx not in cart_set) and (lx not in emp_set)

            candidatos = [c for c in (df_cred["credor"].dropna().tolist() if not df_cred.empty else []) if eh_boleto_nome(c)]
            if not candidatos:
                return []
            df = pd.DataFrame({"rotulo": candidatos})
            df["key"] = df["rotulo"].str.lower().str.strip()
            df = df[df["key"] != ""].drop_duplicates("key", keep="first")
            return df["rotulo"].sort_values().tolist()

    return []


# ---------------- Helpers de coluna de valor/saldo (para mostrar "valor em aberto")
def _resolver_coluna_preferida(conn, preferidas: list[str]) -> Optional[str]:
    cols = pd.read_sql("PRAGMA table_info(contas_a_pagar_mov)", conn)
    existentes = set((cols["name"] if "name" in cols.columns else []).tolist())
    for c in preferidas:
        if c in existentes:
            return c
    return None


def _resolver_colunas_evento_e_pago(conn) -> tuple[str, Optional[str]]:
    """
    Retorna (col_valor_evento, col_valor_pago_acumulado_ou_None)

    Base: prioriza SEMPRE `valor_evento`.
    Se não existir, cai para colunas conhecidas de valor.

    Pago acumulado: tenta colunas comuns de acumulado.
    """
    preferidas_evento = [
        "valor_evento",  # preferida
        "saldo", "valor_saldo", "valor_em_aberto",
        "valor_a_pagar", "valor_previsto", "valor_original", "valor"
    ]
    preferidas_pago_acum = [
        "valor_pago_acumulado", "pago_acumulado",
        "total_pago", "valor_pago_total", "pago_total",
        "valor_pago", "pago"
    ]

    col_evento = _resolver_coluna_preferida(conn, preferidas_evento) or "valor"
    col_pago   = _resolver_coluna_preferida(conn, preferidas_pago_acum)  # pode ser None

    return col_evento, col_pago


# ---------------- Providers NOVOS (mostram VALOR EM ABERTO no label)
def _listar_boletos_em_aberto(caminho_banco: str) -> list[dict]:
    """
    Lista parcelas de BOLETO em aberto/parcial, mostrando o VALOR EM ABERTO no label.
    em_aberto = valor_evento - valor_pago_acumulado
    Saída: [{label, obrigacao_id, parcela_id, credor, vencimento, valor_evento, pago_acumulado, em_aberto}]
    """
    with get_conn(caminho_banco) as conn:
        col_evento, col_pago = _resolver_colunas_evento_e_pago(conn)
        sel_pago = f", COALESCE({col_pago}, 0.0) AS valor_pago_acum" if col_pago else ", 0.0 AS valor_pago_acum"

        df = pd.read_sql(
            f"""
            SELECT
                id                           AS parcela_id,
                COALESCE(obrigacao_id, 0)    AS obrigacao_id,
                TRIM(COALESCE(credor, ''))   AS credor,
                COALESCE(parcela_num, 1)     AS parcela_num,
                COALESCE(parcelas_total, 1)  AS parcelas_total,
                DATE(vencimento)             AS vencimento,
                COALESCE({col_evento}, 0.0)  AS valor_evento
                {sel_pago},
                UPPER(TRIM(COALESCE(tipo_obrigacao,''))) AS u_tipo
            FROM contas_a_pagar_mov
            WHERE UPPER(COALESCE(status, 'EM ABERTO')) IN ('EM ABERTO', 'PARCIAL')
            ORDER BY DATE(vencimento) ASC, credor ASC, parcela_num ASC
            """,
            conn,
        )

    if df is None or df.empty:
        return []

    # aceita BOLETO ou variações que começam com 'BOLETO'
    df = df[(df["u_tipo"] == "BOLETO") | (df["u_tipo"].str.startswith("BOLETO"))]
    if df.empty:
        return []

    # calcula em_aberto = max(valor_evento - valor_pago_acum, 0)
    df["em_aberto"] = (df["valor_evento"] - df["valor_pago_acum"]).clip(lower=0.0)

    # 🔒 Hotfix: remover registros “fantasma” com em_aberto <= 0
    df = df[df["em_aberto"] > 0.0]

    def _fmt_row(r):
        credor = (r["credor"] or "").strip() or "(sem credor)"
        par    = int(r["parcela_num"] or 1)
        tot    = int(r["parcelas_total"] or par)
        venc   = str(r["vencimento"] or "")
        try:
            venc_pt = pd.to_datetime(venc).strftime("%d/%m/%Y") if venc else "—"
        except Exception:
            venc_pt = "—"
        em_aberto = float(r["em_aberto"] or 0.0)
        rotulo = f"{credor} • Parc {par}/{tot} • Venc {venc_pt} • R$ {em_aberto:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return {
            "label": rotulo,
            "obrigacao_id": int(r["obrigacao_id"] or 0),
            "parcela_id": int(r["parcela_id"]),
            "credor": credor,
            "vencimento": venc,
            "valor_evento": float(r["valor_evento"] or 0.0),
            "pago_acumulado": float(r["valor_pago_acum"] or 0.0),
            "em_aberto": em_aberto,
            "parcela_num": par,
            "parcelas_total": tot,
        }

    return [_fmt_row(r) for _, r in df.iterrows()]


def _listar_empfin_em_aberto(caminho_banco: str) -> list[dict]:
    """
    Lista parcelas de EMPRÉSTIMO/FINANCIAMENTO em aberto/parcial, mostrando o VALOR EM ABERTO.
    em_aberto = valor_evento - valor_pago_acumulado
    Saída: [{label, obrigacao_id, parcela_id, credor, vencimento, valor_evento, pago_acumulado, em_aberto}]
    """
    with get_conn(caminho_banco) as conn:
        col_evento, col_pago = _resolver_colunas_evento_e_pago(conn)
        sel_pago = f", COALESCE({col_pago}, 0.0) AS valor_pago_acum" if col_pago else ", 0.0 AS valor_pago_acum"

        df = pd.read_sql(
            f"""
            SELECT
                id                           AS parcela_id,
                COALESCE(obrigacao_id, 0)    AS obrigacao_id,
                TRIM(
                    COALESCE(
                        NULLIF(TRIM(credor), ''),
                        NULLIF(TRIM(descricao), ''),
                        'Empréstimo'
                    )
                )                             AS credor,
                COALESCE(parcela_num, 1)     AS parcela_num,
                COALESCE(parcelas_total, 1)  AS parcelas_total,
                DATE(vencimento)             AS vencimento,
                COALESCE({col_evento}, 0.0)  AS valor_evento
                {sel_pago},
                UPPER(TRIM(REPLACE(COALESCE(tipo_obrigacao,''),'É','E'))) AS u_tipo_norm
            FROM contas_a_pagar_mov
            WHERE UPPER(COALESCE(status, 'EM ABERTO')) IN ('EM ABERTO', 'PARCIAL')
            ORDER BY DATE(vencimento) ASC, credor ASC, parcela_num ASC
            """,
            conn,
        )

    if df is None or df.empty:
        return []

    df = df[(df["u_tipo_norm"] == "EMPRESTIMO") | (df["u_tipo_norm"].str.startswith("EMPR"))]
    if df.empty:
        return []

    df["em_aberto"] = (df["valor_evento"] - df["valor_pago_acum"]).clip(lower=0.0)

    # 🔒 Mesmo hotfix dos boletos: remover registros com em_aberto <= 0
    df = df[df["em_aberto"] > 0.0]

    def _fmt_row(r):
        credor = (r["credor"] or "").strip() or "Empréstimo"
        par    = int(r["parcela_num"] or 1)
        tot    = int(r["parcelas_total"] or par)
        venc   = str(r["vencimento"] or "")
        try:
            venc_pt = pd.to_datetime(venc).strftime("%d/%m/%Y") if venc else "—"
        except Exception:
            venc_pt = "—"
        em_aberto = float(r["em_aberto"] or 0.0)
        rotulo = f"{credor} • Parc {par}/{tot} • Venc {venc_pt} • R$ {em_aberto:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return {
            "label": rotulo,
            "obrigacao_id": int(r["obrigacao_id"] or 0),
            "parcela_id": int(r["parcela_id"]),
            "credor": credor,
            "vencimento": venc,
            "valor_evento": float(r["valor_evento"] or 0.0),
            "pago_acumulado": float(r["valor_pago_acum"] or 0.0),
            "em_aberto": em_aberto,
            "parcela_num": par,
            "parcelas_total": tot,
        }

    return [_fmt_row(r) for _, r in df.iterrows()]


# ---------------- Resultado
class ResultadoSaida(TypedDict):
    ok: bool
    msg: str


# ---------------- Carregamentos para a UI (listas)
def carregar_listas_para_form(
    caminho_banco: str,
) -> Tuple[
    list[str],                          # nomes_bancos
    list[str],                          # nomes_cartoes
    pd.DataFrame,                       # df_categorias
    Callable[[int], pd.DataFrame],      # listar_subcategorias(cat_id)->DataFrame
    Callable[[], list[dict]],           # listar_destinos_fatura_em_aberto()->list[dict]
    Callable[[str], list[str]],         # _opcoes_pagamentos(tipo)->list[str] (legacy)
    Callable[[], list[dict]],           # listar_boletos_em_aberto()->list[dict]
    Callable[[], list[dict]],           # listar_empfin_em_aberto()->list[dict]
]:
    """
    Carrega listas necessárias para o formulário.
    Returns:
      (nomes_bancos, nomes_cartoes, df_categorias,
       listar_subcategorias_fn,
       listar_destinos_fatura_em_aberto_fn,
       carregar_opcoes_pagamentos_fn,  # compat
       listar_boletos_em_aberto_fn,
       listar_empfin_em_aberto_fn)
    """
    bancos_repo = BancoRepository(caminho_banco)
    cartoes_repo = CartoesRepository(caminho_banco)
    cats_repo = CategoriasRepository(caminho_banco)

    df_bancos = bancos_repo.carregar_bancos()
    nomes_bancos = df_bancos["nome"].tolist() if df_bancos is not None and not df_bancos.empty else []
    nomes_cartoes = cartoes_repo.listar_nomes()
    df_categorias = cats_repo.listar_categorias()

    return (
        nomes_bancos,
        nomes_cartoes,
        df_categorias,
        cats_repo.listar_subcategorias,                       # fn(cat_id)->DataFrame
        lambda: listar_destinos_fatura_em_aberto(caminho_banco),  # fn()->list[dict]
        lambda tipo: _opcoes_pagamentos(caminho_banco, tipo),     # legacy/compat
        lambda: _listar_boletos_em_aberto(caminho_banco),         # fn()->list[dict]
        lambda: _listar_empfin_em_aberto(caminho_banco),          # fn()->list[dict]
    )


# ---------------- Dispatcher principal (mantém as mesmas regras do original)
def registrar_saida(caminho_banco: str, data_lanc: date, usuario_nome: str, payload: dict) -> ResultadoSaida:
    """
    Dispatcher que executa a mesma lógica do módulo original, com as mesmas validações.
    """
    ledger = LedgerService(caminho_banco)
    cartoes_repo = CartoesRepository(caminho_banco)

    # Unpack do payload (nomes idênticos aos usados no original/UI)
    valor_saida = float(payload.get("valor_saida") or 0.0)
    forma_pagamento = (payload.get("forma_pagamento") or "").strip()
    cat_nome = (payload.get("cat_nome") or "").strip()
    subcat_nome = (payload.get("subcat_nome") or "").strip()
    is_pagamentos = bool(payload.get("is_pagamentos"))
    tipo_pagamento_sel = (payload.get("tipo_pagamento_sel") or "").strip() if is_pagamentos else None
    destino_pagamento_sel = (payload.get("destino_pagamento_sel") or "").strip() if is_pagamentos else None

    # Fatura
    competencia_fatura_sel = payload.get("competencia_fatura_sel")
    obrigacao_id_fatura = payload.get("obrigacao_id_fatura")
    multa_fatura = float(payload.get("multa_fatura") or 0.0)
    juros_fatura = float(payload.get("juros_fatura") or 0.0)
    desconto_fatura = float(payload.get("desconto_fatura") or 0.0)

    # Boleto
    parcela_boleto_escolhida = payload.get("parcela_boleto_escolhida")  # dict da parcela selecionada
    multa_boleto = float(payload.get("multa_boleto") or 0.0)
    juros_boleto = float(payload.get("juros_boleto") or 0.0)
    desconto_boleto = float(payload.get("desconto_boleto") or 0.0)

    # Empréstimo
    parcela_emp_escolhida = payload.get("parcela_emp_escolhida")        # dict da parcela selecionada
    multa_emp = float(payload.get("multa_emp") or 0.0)
    juros_emp = float(payload.get("juros_emp") or 0.0)
    desconto_emp = float(payload.get("desconto_emp") or 0.0)

    # Demais campos
    parcelas = int(payload.get("parcelas") or 1)
    cartao_escolhido = (payload.get("cartao_escolhido") or "").strip()
    banco_escolhido_in = (payload.get("banco_escolhido") or "").strip()
    origem_dinheiro = (payload.get("origem_dinheiro") or "").strip()
    venc_1 = payload.get("venc_1")
    fornecedor = (payload.get("fornecedor") or "").strip()
    documento = (payload.get("documento") or "").strip()
    descricao_final = (payload.get("descricao_final") or "").strip()

    data_str = str(data_lanc)

    # ================== Validações gerais/finais ==================
    if is_pagamentos and tipo_pagamento_sel == "Boletos":
        valor_digitado = float(valor_saida)
        if valor_digitado <= 0 and (multa_boleto + juros_boleto - desconto_boleto) <= 0:
            raise ValueError("Informe um valor de pagamento > 0 ou ajustes (multa/juros/desconto).")

    if is_pagamentos and tipo_pagamento_sel == "Fatura Cartão de Crédito":
        valor_digitado = float(valor_saida)
        if valor_digitado <= 0 and (multa_fatura + juros_fatura - desconto_fatura) <= 0:
            raise ValueError("Informe um valor de pagamento > 0 ou ajustes (multa/juros/desconto).")

    if is_pagamentos and tipo_pagamento_sel == "Empréstimos e Financiamentos":
        valor_digitado = float(valor_saida)
        if valor_digitado <= 0 and (multa_emp + juros_emp - desconto_emp) <= 0:
            raise ValueError("Informe um valor de pagamento > 0 ou ajustes (multa/juros/desconto).")

    if not is_pagamentos and valor_saida <= 0:
        raise ValueError("O valor deve ser maior que zero.")

    # Validações específicas dos fluxos
    if forma_pagamento in ["PIX", "DÉBITO"] and not banco_escolhido_in:
        raise ValueError("Selecione ou digite o banco da saída.")
    if forma_pagamento == "DINHEIRO" and not origem_dinheiro:
        raise ValueError("Informe a origem do dinheiro (Caixa/Caixa 2).")

    if is_pagamentos:
        if not tipo_pagamento_sel:
            raise ValueError("Selecione o tipo de pagamento (Fatura, Empréstimos ou Boletos).")
        if tipo_pagamento_sel != "Fatura Cartão de Crédito":
            if not destino_pagamento_sel or not str(destino_pagamento_sel).strip():
                raise ValueError("Selecione o destino correspondente ao tipo escolhido.")
        else:
            if not obrigacao_id_fatura:
                raise ValueError("Selecione uma fatura em aberto (cartão • mês • saldo).")

    # ================== Branches Especiais (Pagamentos) ==================
    if is_pagamentos and tipo_pagamento_sel == "Boletos":
        if not destino_pagamento_sel or not str(destino_pagamento_sel).strip():
            raise ValueError("Selecione o credor do boleto.")
        if not parcela_boleto_escolhida:
            raise ValueError("Selecione a parcela do boleto para pagar (ou informe o identificador).")

        obrigacao_id = (
            payload.get("obrigacao_id")
            or payload.get("parcela_obrigacao_id")
            or (parcela_boleto_escolhida.get("obrigacao_id") if isinstance(parcela_boleto_escolhida, dict) else None)
            or 0
        )

        origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)
        id_saida, id_mov, id_cap = ledger.pagar_parcela_boleto(
            data=data_str,
            valor=float(valor_saida),
            forma_pagamento=forma_pagamento,
            origem=origem,
            obrigacao_id=int(obrigacao_id),
            usuario=usuario_nome,
            categoria="Boletos",
            sub_categoria=subcat_nome,
            descricao=descricao_final,
            descricao_extra_cap=f"{destino_pagamento_sel}",
            multa=float(multa_boleto),
            juros=float(juros_boleto),
            desconto=float(desconto_boleto),
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1 or id_mov == -1 or id_cap == -1
                else f"✅ Pagamento de boleto registrado! Valor: {valor_saida:.2f} | Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
            ),
        }

    if is_pagamentos and tipo_pagamento_sel == "Fatura Cartão de Crédito":
        origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)
        id_saida, id_mov, id_cap = ledger.pagar_fatura_cartao(
            data=data_str,
            valor=float(valor_saida),
            forma_pagamento=forma_pagamento,
            origem=origem,
            obrigacao_id=int(obrigacao_id_fatura),
            usuario=usuario_nome,
            categoria="Fatura Cartão de Crédito",
            sub_categoria=subcat_nome,
            descricao=descricao_final,
            multa=float(multa_fatura),
            juros=float(juros_fatura),
            desconto=float(desconto_fatura),
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1 or id_mov == -1 or id_cap == -1
                else f"✅ Pagamento de fatura registrado! Valor: {valor_saida:.2f} | Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
            ),
        }

    if is_pagamentos and tipo_pagamento_sel == "Empréstimos e Financiamentos":
        if not destino_pagamento_sel:
            raise ValueError("Selecione o banco/descrição do empréstimo.")
        if not parcela_emp_escolhida:
            raise ValueError("Selecione a parcela do empréstimo (ou informe o identificador).")

        obrigacao_id = (
            payload.get("obrigacao_id")
            or payload.get("parcela_obrigacao_id")
            or (parcela_emp_escolhida.get("obrigacao_id") if isinstance(parcela_emp_escolhida, dict) else None)
            or 0
        )

        origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)
        id_saida, id_mov, id_cap = ledger.pagar_parcela_emprestimo(
            data=data_str,
            valor=float(valor_saida),
            forma_pagamento=forma_pagamento,
            origem=origem,
            obrigacao_id=int(obrigacao_id),
            usuario=usuario_nome,
            categoria="Empréstimos e Financiamentos",
            sub_categoria=subcat_nome,
            descricao=descricao_final,
            multa=float(multa_emp),
            juros=float(juros_emp),
            desconto=float(desconto_emp),
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1 or id_mov == -1 or id_cap == -1
                else f"✅ Parcela de Empréstimo paga! Valor: {valor_saida:.2f} | Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
            ),
        }

    # ================== Fluxos Padrão ==================
    categoria = cat_nome
    sub_categoria = subcat_nome

    if forma_pagamento == "DINHEIRO":
        id_saida, id_mov = ledger.registrar_saida_dinheiro(
            data=data_str,
            valor=float(valor_saida),
            origem_dinheiro=origem_dinheiro,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao_final,
            usuario=usuario_nome,
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1
                else f"✅ Saída em dinheiro registrada! Valor: {valor_saida:.2f} | ID saída: {id_saida} | Log: {id_mov}"
            ),
        }

    if forma_pagamento in ["PIX", "DÉBITO"]:
        banco_nome = _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)
        id_saida, id_mov = ledger.registrar_saida_bancaria(
            data=data_str,
            valor=float(valor_saida),
            banco_nome=banco_nome,
            forma=forma_pagamento,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao_final,
            usuario=usuario_nome,
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1
                else f"✅ Saída bancária ({forma_pagamento}) registrada! Valor: {valor_saida:.2f} | ID saída: {id_saida} | Log: {id_mov}"
            ),
        }

    if forma_pagamento == "CRÉDITO":
        fc_vc = cartoes_repo.obter_por_nome(cartao_escolhido)
        if not fc_vc:
            raise ValueError("Cartão não encontrado. Cadastre em 📇 Cartão de Crédito.")
        vencimento, fechamento = fc_vc  # ordem preservada

        ids_fatura, id_mov = ledger.registrar_saida_credito(
            data_compra=data_str,
            valor=float(valor_saida),
            parcelas=int(parcelas),
            cartao_nome=cartao_escolhido,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao_final,   # descrição detalhada para fatura_cartao_itens
            usuario=usuario_nome,
            fechamento=int(fechamento),
            vencimento=int(vencimento),
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if not ids_fatura
                else f"✅ Despesa em CRÉDITO programada! Valor: {valor_saida:.2f} | Parcelas criadas: {len(ids_fatura)} | Log: {id_mov}"
            ),
        }

    if forma_pagamento == "BOLETO":
        ids_cap, id_mov = ledger.registrar_saida_boleto(
            data_compra=data_str,
            valor=float(valor_saida),
            parcelas=int(parcelas),
            vencimento_primeira=str(venc_1),
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao_final,
            usuario=usuario_nome,
            fornecedor=(fornecedor or None),
            documento=(documento or None),
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if not ids_cap
                else f"✅ Boleto programado! Valor: {valor_saida:.2f} | Parcelas criadas: {len(ids_cap)} | Log: {id_mov}"
            ),
        }

    # Se chegou aqui, forma desconhecida
    raise ValueError("Forma de pagamento inválida ou não suportada.")


# ---------------- Canonicalização de banco (igual ao original, tolerante a falha)
def _canonicalizar_banco_safe(caminho_banco: str, banco_in: str) -> str:
    try:
        return canonicalizar_banco(caminho_banco, (banco_in or "").strip()) or (banco_in or "").strip()
    except Exception:
        return (banco_in or "").strip()
