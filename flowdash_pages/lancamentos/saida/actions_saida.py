# ===================== Actions: Saída =====================
"""
Actions da página de Saída (sem Streamlit aqui).

Fluxos suportados (mesmos do original):
- Padrão: DINHEIRO, PIX/DÉBITO, CRÉDITO, BOLETO (programação)
- Pagamentos: Fatura Cartão, Boletos (parcela), Empréstimos (parcela)

Compatibilidade e decisões:
- Mantém canonicalização de banco e validações equivalentes (erros via Exceptions).
- Fatura:
    • Se existir `ledger.pagar_fatura_cartao` (legado), usa-o.
    • Caso contrário, faz fallback criando a saída (dinheiro/bancária) com `obrigacao_id_fatura`,
      o que aplica o pagamento e retorna IDs de saída/log.
- Boletos/Empréstimos:
    • Aceita retorno legado (tupla com IDs) **ou** retorno novo (dict). Calcule restante/status
      diretamente no banco quando precisar.
- Valor em aberto/Status:
    • Preferimos `principal_pago_acumulado` (padrão novo). Se não existir, caímos para
      `valor_pago_acumulado` (legado).

OBS: As funções daqui não importam Streamlit — UI cuida da exibição/try-catch.
"""

from __future__ import annotations

from typing import TypedDict, Optional, Callable, Tuple, Any, Dict
from datetime import date, datetime
import pandas as pd

from shared.db import get_conn
# Import estável do LedgerService
from services.ledger.service_ledger import LedgerService

from repository.cartoes_repository import CartoesRepository
from repository.categorias_repository import CategoriasRepository
from flowdash_pages.cadastros.cadastro_classes import BancoRepository
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository  # compat
from flowdash_pages.lancamentos.shared_ui import canonicalizar_banco  # usado no original

__all__ = [
    "carregar_listas_para_form",
    "registrar_saida",
    "listar_boletos_em_aberto",
    "_listar_boletos_em_aberto",
]

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
    """Compat legada para opções de 'Pagamentos' antigas na UI."""
    with get_conn(caminho_banco) as conn:
        if tipo == "Fatura Cartão de Crédito":
            return []

        if tipo == "Empréstimos e Financiamentos":
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
            ).dropna()
            df_emp = df_emp[df_emp["rotulo"] != ""]
            return _distinct_lower_trim(df_emp["rotulo"]) if not df_emp.empty else []

        if tipo == "Boletos":
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


# ---------- Contador seguro (lista/tupla/set → len; int/float truthy → 1; None/falsy → 0)
def _safe_count(x: Any) -> int:
    if x is None:
        return 0
    if isinstance(x, (list, tuple, set)):
        return len(x)
    # alguns serviços antigos retornam 1 id inteiro (ou 0/None)
    try:
        # int/float/bool
        return 1 if int(x) != 0 else 0
    except Exception:
        # último recurso: tenta len genérico
        try:
            return len(x)  # type: ignore[arg-type]
        except Exception:
            return 0


# ---------------- Helpers de coluna de valor/saldo (para mostrar "valor em aberto")
def _resolver_coluna_preferida(conn, preferidas: list[str]) -> Optional[str]:
    cols = pd.read_sql("PRAGMA table_info(contas_a_pagar_mov)", conn)
    existentes = set((cols["name"] if "name" in cols.columns else []).tolist())
    for c in preferidas:
        if c in existentes:
            return c
    return None


def _resolver_colunas_evento_e_pago(conn) -> tuple[str, Optional[str], Optional[str]]:
    """
    Retorna (col_valor_evento, col_principal_pago_acumulado_ou_None, col_valor_pago_acumulado_ou_None)

    Prioriza SEMPRE `valor_evento` e, para pago, primeiro `principal_pago_acumulado` (padrão novo).
    """
    preferidas_evento = [
        "valor_evento",  # preferida
        "saldo", "valor_saldo", "valor_em_aberto",
        "valor_a_pagar", "valor_previsto", "valor_original", "valor"
    ]
    preferidas_principal_acum = [
        "principal_pago_acumulado", "principal_acumulado", "principal_pago"
    ]
    preferidas_valor_acum = [
        "valor_pago_acumulado", "pago_acumulado", "total_pago",
        "valor_pago_total", "pago_total", "valor_pago", "pago"
    ]

    col_evento = _resolver_coluna_preferida(conn, preferidas_evento) or "valor"
    col_princ  = _resolver_coluna_preferida(conn, preferidas_principal_acum)
    col_pago   = _resolver_coluna_preferida(conn, preferidas_valor_acum)
    return col_evento, col_princ, col_pago


# ---------------- Providers NOVOS (mostram VALOR EM ABERTO no label)

def _listar_empfin_em_aberto(caminho_banco: str) -> list[dict]:
    """
    Lista parcelas de EMPRESTIMO/FINANCIAMENTO em aberto/parcial, mostrando o VALOR EM ABERTO.
    em_aberto = valor_evento - (principal_pago_acumulado OU valor_pago_acumulado)
    """
    with get_conn(caminho_banco) as conn:
        col_evento, col_princ, col_pago = _resolver_colunas_evento_e_pago(conn)
        sel_pago = f", COALESCE({col_princ}, 0.0) AS principal_pago_acum" if col_princ else ""
        sel_pago += f", COALESCE({col_pago}, 0.0) AS valor_pago_acum" if col_pago else ", 0.0 AS valor_pago_acum"

        df = pd.read_sql(
            f"""
            SELECT
                id                           AS parcela_id,
                COALESCE(obrigacao_id, 0)    AS obrigacao_id,
                TRIM(
                    COALESCE(
                        NULLIF(TRIM(credor), ''), NULLIF(TRIM(descricao), ''), 'Empréstimo'
                    )
                )                             AS credor,
                COALESCE(parcela_num, 1)     AS parcela_num,
                COALESCE(parcelas_total, 1)  AS parcelas_total,
                DATE(vencimento)             AS vencimento,
                COALESCE({col_evento}, 0.0)  AS valor_evento
                {sel_pago},
                UPPER(TRIM(REPLACE(COALESCE(tipo_obrigacao,''),'É','E'))) AS u_tipo_norm
            FROM contas_a_pagar_mov
            WHERE categoria_evento = 'LANCAMENTO'
              AND UPPER(COALESCE(status, 'EM ABERTO')) IN ('EM ABERTO', 'PARCIAL')
            ORDER BY DATE(vencimento) ASC, credor ASC, parcela_num ASC
            """,
            conn,
        )

    if df is None or df.empty:
        return []

    df = df[(df["u_tipo_norm"] == "EMPRESTIMO") | (df["u_tipo_norm"].str.startswith("EMPR"))]
    if df.empty:
        return []

    base_pago = df["principal_pago_acum"] if "principal_pago_acum" in df.columns else df["valor_pago_acum"]
    df["em_aberto"] = (df["valor_evento"] - base_pago).clip(lower=0.0)
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
            "pago_acumulado": float(base_pago.loc[r.name] or 0.0),
            "em_aberto": em_aberto,
            "parcela_num": par,
            "parcelas_total": tot,
        }

    return [_fmt_row(r) for _, r in df.iterrows()]


# ---------------- Resultado
class ResultadoSaida(TypedDict):
    ok: bool
    msg: str


def _listar_faturas_cartao_abertas_dropdown(caminho_banco: str) -> list[dict]:
    """Monta as opções de dropdown de faturas usando o repositório CAP (padrão novo)."""
    repo = ContasAPagarMovRepository(caminho_banco)
    faturas = repo.listar_faturas_cartao_abertas()
    opcoes = []
    for f in faturas:
        credor = (f.get("credor") or "").strip()
        comp   = (f.get("competencia") or "").strip()
        if credor or comp:
            titulo = f"{credor} {comp}".strip()
        else:
            desc = (f.get("descricao") or "").strip()
            data = (f.get("data_evento") or "").strip()
            titulo = f"{desc} {data}".strip()
        rotulo = f"Fatura  {titulo} — R$ {float(f.get('saldo_restante', 0.0)):.2f}"
        opcoes.append({
            "label": rotulo,
            "parcela_id": int(f["parcela_id"]),
            "obrigacao_id": int(f["obrigacao_id"]),
            "saldo_restante": float(f.get("saldo_restante", 0.0)),
            "valor_total": float(f.get("valor_total", 0.0)),
            "status": f.get("status", "Em aberto"),
            "credor": credor,
            "competencia": comp,
            "raw": f,
        })
    return opcoes


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
    """Carrega listas e providers necessários para o formulário de Saída."""
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
        cats_repo.listar_subcategorias,
        lambda: _listar_faturas_cartao_abertas_dropdown(caminho_banco),
        lambda tipo: _opcoes_pagamentos(caminho_banco, tipo),
        lambda: _listar_boletos_em_aberto(caminho_banco),
        lambda: _listar_empfin_em_aberto(caminho_banco),
    )


# ---------------- Util: restante/status pós-pagamento (pelo banco, com fallback)
def _obter_restante_e_status(caminho_banco: str, obrigacao_id: int) -> tuple[float, str]:
    """Calcula restante/status pós-pagamento consultando o CAP.

    Preferência:
      faltante = valor_evento - principal_pago_acumulado
    Fallback:
      faltante = valor_evento - valor_pago_acumulado
    """
    restante = 0.0
    status = "QUITADA"
    try:
        with get_conn(caminho_banco) as _c:
            col_evento, col_princ, col_pago = _resolver_colunas_evento_e_pago(_c)
            select_princ = f"COALESCE({col_princ}, NULL)" if col_princ else "NULL"
            select_pago  = f"COALESCE({col_pago}, NULL)" if col_pago else "NULL"

            _row = pd.read_sql(
                f"""
                SELECT
                    COALESCE({col_evento}, 0.0) AS valor_evento,
                    {select_princ} AS principal_acum,
                    {select_pago}  AS valor_pago_acum
                  FROM contas_a_pagar_mov
                 WHERE obrigacao_id = ?
                   AND categoria_evento = 'LANCAMENTO'
                 LIMIT 1
                """,
                _c,
                params=(int(obrigacao_id),),
            )

        if not _row.empty:
            ve = float(_row.iloc[0]["valor_evento"] or 0.0)
            if pd.notna(_row.iloc[0]["principal_acum"]):
                pago_base = float(_row.iloc[0]["principal_acum"] or 0.0)
            elif pd.notna(_row.iloc[0]["valor_pago_acum"]):
                pago_base = float(_row.iloc[0]["valor_pago_acum"] or 0.0)
            else:
                pago_base = 0.0

            _rest = ve - pago_base
            restante = round(_rest if _rest > 0 else 0.0, 2)
            status = "PARCIAL" if restante > 0.005 else "QUITADA"
    except Exception:
        pass
    return restante, status


# ---------------- Dispatcher principal (mantém as mesmas regras do original)
def registrar_saida(caminho_banco: str, data_lanc: date, usuario_nome: str, payload: dict) -> ResultadoSaida:
    """Executa a mesma lógica do módulo original com compat das camadas novas/legadas."""
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
        # Origem (para fallback por registrar_saida_*)
        origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)

        # ===== Tenta fluxo legado (se existir) =====
        if hasattr(ledger, "pagar_fatura_cartao"):
            try:
                res: Any = ledger.pagar_fatura_cartao(
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
                    retornar_info=True,  # alguns legados aceitam
                )
            except TypeError:
                # Sem o kw extra
                res = ledger.pagar_fatura_cartao(
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

            # Compat retorno
            if isinstance(res, tuple) and len(res) >= 3:
                id_saida, id_mov, id_cap = res[0], res[1], res[2]
            else:
                id_saida = id_mov = id_cap = None

            restante, status = _obter_restante_e_status(caminho_banco, int(obrigacao_id_fatura))
            return {
                "ok": True,
                "msg": (
                    "ℹ️ Transação já registrada (idempotência)."
                    if id_saida in (-1, None) or id_mov in (-1, None) or id_cap in (-1, None)
                    else f"✅ Pagamento de fatura registrado! Pago (principal): R$ {float(valor_saida):.2f} | Restante: R$ {restante:.2f} | Status: {status} | Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
                ),
            }

        # ===== Fallback novo modelo: cria a saída que paga a fatura =====
        if forma_pagamento == "DINHEIRO":
            id_saida, id_mov = ledger.registrar_saida_dinheiro(
                data=data_str,
                valor=float(valor_saida),
                origem_dinheiro=origem_dinheiro,
                categoria="Fatura Cartão de Crédito",
                sub_categoria=subcat_nome,
                descricao=descricao_final,
                usuario=usuario_nome,
                obrigacao_id_fatura=int(obrigacao_id_fatura),
            )
        else:
            banco_nome = _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)
            id_saida, id_mov = ledger.registrar_saida_bancaria(
                data=data_str,
                valor=float(valor_saida),
                banco_nome=banco_nome,
                forma=forma_pagamento,
                categoria="Fatura Cartão de Crédito",
                sub_categoria=subcat_nome,
                descricao=descricao_final,
                usuario=usuario_nome,
                obrigacao_id_fatura=int(obrigacao_id_fatura),
            )

        restante, status = _obter_restante_e_status(caminho_banco, int(obrigacao_id_fatura))
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1 or id_mov == -1
                else f"✅ Pagamento de fatura registrado! Pago (principal): R$ {float(valor_saida):.2f} | Restante: R$ {restante:.2f} | Status: {status} | Saída: {id_saida or '—'} | Log: {id_mov or '—'}"
            ),
        }

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

        # Compat: serviço pode retornar tupla (legado) ou dict (novo)
        try:
            res: Any = getattr(ledger, "pagar_parcela_boleto")(
                data=data_str,
                valor=float(valor_saida),
                forma_pagamento=forma_pagamento,
                origem=(origem_dinheiro if forma_pagamento == "DINHEIRO" else _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)),
                obrigacao_id=int(obrigacao_id),
                usuario=usuario_nome,
                categoria="Boletos",
                sub_categoria=subcat_nome,
                descricao=descricao_final,
                multa=float(multa_boleto),
                juros=float(juros_boleto),
                desconto=float(desconto_boleto),
            )
        except TypeError:
            # caso assinatura seja diferente (muito legado)
            res = getattr(ledger, "pagar_parcela_boleto")(obrigacao_id=int(obrigacao_id), valor_base=float(valor_saida))

        if isinstance(res, tuple) and len(res) >= 3:
            id_saida, id_mov, id_cap = res[0], res[1], res[2]
        else:
            id_saida = id_mov = id_cap = None  # novo serviço não cria log aqui

        restante, status = _obter_restante_e_status(caminho_banco, int(obrigacao_id))

        if id_saida is None:
            # Mensagem compat com serviço novo (sem IDs)
            return {
                "ok": True,
                "msg": (
                    f"✅ Pagamento de boleto aplicado! "
                    f"Pago (base): R$ {float(valor_saida):.2f} | Restante: R$ {restante:.2f} | Status: {status}"
                ),
            }

        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1 or id_mov == -1 or id_cap == -1
                else (
                    f"✅ Pagamento de boleto registrado! "
                    f"Pago (base): R$ {float(valor_saida):.2f} | "
                    f"Restante: R$ {restante:.2f} | Status: {status} | "
                    f"Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
                )
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

        # Compat: serviço pode retornar tupla (legado) ou dict (novo)
        try:
            res: Any = getattr(ledger, "pagar_parcela_emprestimo")(
                data=data_str,
                valor=float(valor_saida),
                forma_pagamento=forma_pagamento,
                origem=(origem_dinheiro if forma_pagamento == "DINHEIRO" else _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)),
                obrigacao_id=int(obrigacao_id),
                usuario=usuario_nome,
                categoria="Empréstimos e Financiamentos",
                sub_categoria=subcat_nome,
                descricao=descricao_final,
                multa=float(multa_emp),
                juros=float(juros_emp),
                desconto=float(desc__emp) if (desc__emp := desconto_emp) is not None else 0.0,  # robusto
            )
        except TypeError:
            res = getattr(ledger, "pagar_parcela_emprestimo")(obrigacao_id=int(obrigacao_id), valor_base=float(valor_saida))

        if isinstance(res, tuple) and len(res) >= 3:
            id_saida, id_mov, id_cap = res[0], res[1], res[2]
        else:
            id_saida = id_mov = id_cap = None  # novo serviço não cria log aqui

        restante, status = _obter_restante_e_status(caminho_banco, int(obrigacao_id))

        if id_saida is None:
            return {
                "ok": True,
                "msg": (
                    f"✅ Parcela de Empréstimo paga! "
                    f"Pago (base): R$ {float(valor_saida):.2f} | Restante: R$ {restante:.2f} | Status: {status}"
                ),
            }

        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1 or id_mov == -1 or id_cap == -1
                else (
                    f"✅ Parcela de Empréstimo paga! "
                    f"Pago (base): R$ {float(valor_saida):.2f} | "
                    f"Restante: R$ {restante:.2f} | Status: {status} | "
                    f"Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
                )
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
        parcelas_criadas = _safe_count(ids_fatura)
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if parcelas_criadas == 0
                else f"✅ Despesa em CRÉDITO programada! Valor: {valor_saida:.2f} | Parcelas criadas: {parcelas_criadas} | Log: {id_mov}"
            ),
        }

    if forma_pagamento == "BOLETO":
        # Programar parcelas no CAP + LOG
        from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository
        from shared.ids import uid_boleto_programado
        from services.ledger.service_ledger_infra import _ensure_mov_cols, _fmt_obs_saida
        import calendar

        if float(valor_saida) <= 0:
            raise ValueError("O valor deve ser maior que zero.")

        parcelas_n = max(1, int(parcelas or 1))

        def _parse_date_any(s) -> str:
            s = str(s or "").strip().replace("/", "-")
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"):
                try:
                    return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
                except Exception:
                    pass
            return str(data_lanc)

        def _add_months(ymd: str, n: int) -> str:
            y, m, dd = map(int, ymd.split("-"))
            m0 = m - 1 + n
            y2 = y + m0 // 12
            m2 = m0 % 12 + 1
            last = calendar.monthrange(y2, m2)[1]
            d2 = min(dd, last)
            return f"{y2:04d}-{m2:02d}-{d2:02d}"

        venc1 = _parse_date_any(venc_1 or data_str)
        repo = ContasAPagarMovRepository(caminho_banco)

        obrigacao_id = repo.proximo_obrigacao_id(None)
        ids_cap: list[int] = []
        valor_parc = round(float(valor_saida) / parcelas_n, 2)

        for i in range(1, parcelas_n + 1):
            venc = _add_months(venc1, i - 1)
            competencia = venc[:7]  # "YYYY-MM"
            parcela_id = repo.registrar_lancamento(
                None,
                obrigacao_id=obrigacao_id,
                tipo_obrigacao="BOLETO",
                valor_total=valor_parc,
                data_evento=data_str,
                vencimento=venc,
                descricao=descricao_final or None,
                credor=(fornecedor or None),
                competencia=competencia,
                parcela_num=i,
                parcelas_total=parcelas_n,
                usuario=usuario_nome,
                tipo_origem="BOLETO",
                cartao_id=None,
                emprestimo_id=None,
            )
            ids_cap.append(int(parcela_id))

        trans_uid = uid_boleto_programado(
            data_str,
            float(valor_saida),
            parcelas_n,
            venc1,
            categoria or "",
            sub_categoria or "",
            descricao_final or "",
            usuario_nome or "-",
        )

        with get_conn(caminho_banco) as conn:
            cur = conn.cursor()
            _ensure_mov_cols(cur)
            obs = _fmt_obs_saida(
                forma="BOLETO",
                valor=float(valor_saida),
                categoria=categoria,
                subcategoria=sub_categoria,
                descricao=descricao_final,
                banco=(fornecedor or None),  # segue padrão do CRÉDITO programado
            )
            cur.execute(
                """
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao,
                     referencia_tabela, referencia_id, trans_uid, usuario, data_hora)
                VALUES (?, ?, 'saida', ?, 'saidas_boleto_programada', ?, 'contas_a_pagar_mov', ?, ?, ?, ?)
                """,
                (
                    data_str,
                    fornecedor or "",
                    float(valor_saida),
                    obs,
                    ids_cap[0] if ids_cap else None,
                    trans_uid,
                    usuario_nome or "-",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            id_mov = int(cur.lastrowid)
            conn.commit()

        parcelas_criadas = _safe_count(ids_cap)
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if parcelas_criadas == 0
                else f"✅ Boleto programado! Valor: {float(valor_saida):.2f} | Parcelas criadas: {parcelas_criadas} | Obrigação: {obrigacao_id} | Log: {id_mov}"
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


# --- LISTAGEM: Boletos em aberto (privada p/ providers) ----------------------
def _listar_boletos_em_aberto(caminho_banco: str) -> list[dict]:
    return listar_boletos_em_aberto(caminho_banco)


# --- LISTAGEM: Boletos em aberto (pública/reutilizável) ----------------------
def listar_boletos_em_aberto(caminho_banco: str) -> list[dict]:
    """
    Lista parcelas de BOLETO em aberto, mostrando o VALOR EM ABERTO.
    em_aberto = valor_evento - (principal_pago_acumulado OU valor_pago_acumulado)
    """
    eps = 0.005
    with get_conn(caminho_banco) as conn:
        col_evento, col_princ, col_pago = _resolver_colunas_evento_e_pago(conn)
        sel_princ = f", COALESCE({col_princ}, 0.0) AS principal_pago_acum" if col_princ else ", NULL AS principal_pago_acum"
        sel_pago  = f", COALESCE({col_pago}, 0.0) AS valor_pago_acum" if col_pago else ", 0.0 AS valor_pago_acum"

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
                {sel_princ}
                {sel_pago},
                UPPER(TRIM(COALESCE(tipo_obrigacao,''))) AS u_tipo
            FROM contas_a_pagar_mov
            WHERE categoria_evento = 'LANCAMENTO'
              AND UPPER(COALESCE(status, 'EM ABERTO')) IN ('EM ABERTO', 'PARCIAL')
            ORDER BY DATE(vencimento) ASC, credor ASC, parcela_num ASC
            """,
            conn,
        )

    if df is None or df.empty:
        return []

    df = df[(df["u_tipo"] == "BOLETO") | (df["u_tipo"].str.startswith("BOLETO"))]
    if df.empty:
        return []

    base_pago = df["principal_pago_acum"] if "principal_pago_acum" in df.columns and df["principal_pago_acum"].notna().any() else df["valor_pago_acum"]
    df["em_aberto"] = (df["valor_evento"] - base_pago).clip(lower=0.0)
    df = df[df["em_aberto"] > eps]  # evita listar valores residuais por arredondamento

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
            "pago_acumulado": float(base_pago.loc[r.name] or 0.0),
            "em_aberto": em_aberto,
            "parcela_num": par,
            "parcelas_total": tot,
        }

    return [_fmt_row(r) for _, r in df.iterrows()]
