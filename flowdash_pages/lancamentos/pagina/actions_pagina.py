# ===================== Actions: Página de Lançamentos =====================
"""
Consulta o SQLite e calcula os dados do resumo do dia.

Notas:
    - Deduplicação por transações em alguns blocos usa COALESCE(trans_uid, CAST(id AS TEXT)).
    - Para transferências banco→banco, o pareamento SAÍDA/ENTRADA é feito por:
        1) referência cruzada via referencia_id (usa MIN(id, referencia_id) como chave)
        2) fallback TX= existente na observação (lançamentos antigos)
        3) fallback trans_uid (se existir na tabela)
        4) fallback final: id (não pareia, mas não quebra)
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from shared.db import get_conn


# ===================== API =====================
def carregar_resumo_dia(caminho_banco: str, data_lanc) -> Dict[str, Any]:
    """Carrega totais e listas do dia selecionado.

    Agrega:
        - total_vendas: soma de `entrada.Valor` (DATE(Data) = DATE(data_lanc))
        - total_saidas: soma de `saida.valor` (DATE(data) = DATE(data_lanc))
        - caixa_total/caixa2_total: soma acumulada até a data (tabela `saldos_caixas`)
        - saldos_bancos: soma acumulada por banco até a data (tabela `saldos_bancos`)
        - listas do dia: depósitos, transferências, mercadorias (compras/recebimentos)

    Args:
        caminho_banco: Caminho do arquivo SQLite.
        data_lanc: Data do lançamento (date/datetime/str compatível com DATE()).

    Returns:
        Dicionário com métricas e listas para o resumo do dia.
    """
    total_vendas, total_saidas = 0.0, 0.0
    caixa_total = 0.0
    caixa2_total = 0.0
    transf_caixa2_total = 0.0
    depositos_list: List[Tuple[str, float]] = []
    transf_bancos_list: List[Tuple[str, str, float]] = []
    compras_list: List[Tuple[str, str, float]] = []
    receb_list: List[Tuple[str, str, float]] = []
    saldos_bancos: Dict[str, float] = {}

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # ===== VENDAS do dia (Data da VENDA) =====
        total_vendas = float(
            cur.execute(
                """
                SELECT COALESCE(SUM(COALESCE(Valor,0)), 0.0)
                  FROM entrada
                 WHERE DATE(Data) = DATE(?)
                """,
                (str(data_lanc),),
            ).fetchone()[0]
            or 0.0
        )

        # ===== SAÍDAS do dia =====
        total_saidas = float(
            cur.execute(
                """
                SELECT COALESCE(SUM(COALESCE(valor,0)), 0.0)
                  FROM saida
                 WHERE DATE(data) = DATE(?)
                """,
                (str(data_lanc),),
            ).fetchone()[0]
            or 0.0
        )

        # ===== Caixas: somatório acumulado <= data =====
        try:
            df_cx = pd.read_sql(
                "SELECT data, caixa_total, caixa2_total FROM saldos_caixas", conn
            )
        except Exception:
            df_cx = pd.DataFrame()

        if not df_cx.empty:
            df_cx["data"] = pd.to_datetime(df_cx["data"], errors="coerce")
            df_cx = df_cx[df_cx["data"].dt.date <= data_lanc]
            if "caixa_total" in df_cx.columns:
                caixa_total = float(
                    pd.to_numeric(df_cx["caixa_total"], errors="coerce").fillna(0.0).sum()
                )
            if "caixa2_total" in df_cx.columns:
                caixa2_total = float(
                    pd.to_numeric(df_cx["caixa2_total"], errors="coerce")
                    .fillna(0.0)
                    .sum()
                )

        # ===== Transferência p/ Caixa 2 (dia) — dedupe por trans_uid/id =====
        transf_caixa2_total = float(
            cur.execute(
                """
                SELECT COALESCE(SUM(m.valor), 0.0)
                  FROM movimentacoes_bancarias m
                  JOIN (
                        SELECT MAX(id) AS id
                          FROM movimentacoes_bancarias
                         WHERE DATE(data)=DATE(?)
                           AND origem='transferencia_caixa'
                         GROUP BY COALESCE(trans_uid, CAST(id AS TEXT))
                       ) d ON d.id = m.id
                """,
                (str(data_lanc),),
            ).fetchone()[0]
            or 0.0
        )

        # ===== Depósitos do dia — dedupe por trans_uid/id =====
        depositos_list = cur.execute(
            """
            SELECT m.banco, m.valor
              FROM movimentacoes_bancarias m
              JOIN (
                    SELECT MAX(id) AS id
                      FROM movimentacoes_bancarias
                     WHERE DATE(data)=DATE(?)
                       AND origem='deposito'
                     GROUP BY COALESCE(trans_uid, CAST(id AS TEXT))
                   ) d ON d.id = m.id
             ORDER BY m.id
            """,
            (str(data_lanc),),
        ).fetchall()

        # ===== Transferências banco→banco do dia (pareadas) =====
        pares = listar_transferencias_bancos_do_dia(caminho_banco, data_lanc)
        transf_bancos_list = [
            (p["origem"], p["destino"], float(p["valor"] or 0.0)) for p in pares
        ]

        # ===== Mercadorias do dia (compras) =====
        try:
            df_compras = pd.read_sql(
                "SELECT * FROM mercadorias WHERE DATE(Data)=DATE(?)",
                conn,
                params=(str(data_lanc),),
            )
        except Exception:
            df_compras = pd.DataFrame()

        if not df_compras.empty:
            cols = {c.lower(): c for c in df_compras.columns}
            col_col = cols.get("colecao") or cols.get("coleção")
            col_forn = cols.get("fornecedor")
            col_val = cols.get("valor_mercadoria")
            for _, r in df_compras.iterrows():
                compras_list.append(
                    (
                        str(r.get(col_col, "") if col_col else ""),
                        str(r.get(col_forn, "") if col_forn else ""),
                        float(r.get(col_val, 0) or 0.0) if col_val else 0.0,
                    )
                )

        # ===== Mercadorias do dia (recebimentos) =====
        try:
            df_receb = pd.read_sql(
                """
                SELECT * FROM mercadorias
                 WHERE Recebimento IS NOT NULL
                   AND TRIM(Recebimento) <> ''
                   AND DATE(Recebimento) = DATE(?)
                """,
                conn,
                params=(str(data_lanc),),
            )
        except Exception:
            df_receb = pd.DataFrame()

        if not df_receb.empty:
            cols = {c.lower(): c for c in df_receb.columns}
            col_col = cols.get("colecao") or cols.get("coleção")
            col_forn = cols.get("fornecedor")
            col_vr = cols.get("valor_recebido")
            col_vm = cols.get("valor_mercadoria")
            for _, r in df_receb.iterrows():
                valor = (
                    float(r.get(col_vr))
                    if (col_vr and pd.notna(r.get(col_vr)))
                    else float(r.get(col_vm, 0) or 0.0)
                )
                receb_list.append(
                    (
                        str(r.get(col_col, "") if col_col else ""),
                        str(r.get(col_forn, "") if col_forn else ""),
                        valor,
                    )
                )

        # ===== Saldos bancos (ACUMULADO <= data) =====
        try:
            df_bk = pd.read_sql("SELECT * FROM saldos_bancos", conn)
        except Exception:
            df_bk = pd.DataFrame()

        if not df_bk.empty:
            date_col_name = next((c for c in df_bk.columns if c.lower() == "data"), None)
            if date_col_name:
                df_bk[date_col_name] = pd.to_datetime(
                    df_bk[date_col_name], errors="coerce"
                )
                df_bk = df_bk[df_bk[date_col_name].dt.date <= data_lanc]

            for c in df_bk.columns:
                if c.lower() == "data":
                    continue
                soma = (
                    pd.to_numeric(df_bk[c], errors="coerce")
                    .fillna(0.0)
                    .sum()
                )
                saldos_bancos[str(c)] = float(soma)

    return {
        "total_vendas": total_vendas,
        "total_saidas": total_saidas,
        "caixa_total": caixa_total,
        "caixa2_total": caixa2_total,
        "transf_caixa2_total": transf_caixa2_total,
        "depositos_list": depositos_list,
        "transf_bancos_list": transf_bancos_list,
        "compras_list": compras_list,
        "receb_list": receb_list,
        "saldos_bancos": saldos_bancos,
    }


def listar_transferencias_bancos_do_dia(caminho_banco: str, data_ref) -> List[Dict[str, Any]]:
    """Lista pares de transferências banco→banco do dia.

    Pareamento pela chave `tx`:
        - MIN(id, referencia_id) quando referencia_id estiver preenchido (novo)
        - senão, token `TX=` da observação (retrocompatibilidade)
        - senão, `trans_uid` (se existir)
        - senão, `id` (não pareia, mas não quebra)

    Args:
        caminho_banco: Caminho para o SQLite.
        data_ref: Data de referência (date/datetime/str).

    Returns:
        Lista de dicts: {"origem": str, "destino": str, "valor": float}.
    """
    from utils.utils import coerce_data

    # Normaliza a data para 'YYYY-MM-DD'
    try:
        d = coerce_data(data_ref)
        data_str = d.strftime("%Y-%m-%d")
    except Exception:
        data_str = str(data_ref)

    sql = """
    WITH m AS (
        SELECT
            id,
            referencia_id,
            data,
            banco,
            tipo,
            valor,
            observacao,
            /* chave de pareamento */
            CASE
              WHEN referencia_id IS NOT NULL AND referencia_id > 0 THEN
                CASE WHEN id < referencia_id THEN CAST(id AS TEXT) ELSE CAST(referencia_id AS TEXT) END
              WHEN instr(COALESCE(observacao,''), 'TX=') > 0 THEN
                substr(observacao, instr(observacao, 'TX=') + 3, 36)
              WHEN trans_uid IS NOT NULL AND TRIM(trans_uid) <> '' THEN
                trans_uid
              ELSE
                CAST(id AS TEXT)
            END AS tx
        FROM movimentacoes_bancarias
        WHERE origem = 'transferencia'
          AND DATE(data) = DATE(?)
    )
    SELECT
      MAX(CASE WHEN tipo='saida'   THEN banco END) AS banco_origem,
      MAX(CASE WHEN tipo='entrada' THEN banco END) AS banco_destino,
      ABS(COALESCE(MAX(CASE WHEN tipo='entrada' THEN valor END),
                   MAX(CASE WHEN tipo='saida'   THEN valor END))) AS valor
    FROM m
    GROUP BY tx
    ORDER BY MIN(CASE
                   WHEN referencia_id IS NOT NULL AND referencia_id > 0
                     THEN CASE WHEN id < referencia_id THEN id ELSE referencia_id END
                   ELSE id
                 END);
    """

    with get_conn(caminho_banco) as conn:
        df = pd.read_sql(sql, conn, params=(data_str,))

    if df is None or df.empty:
        return []

    out: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        out.append(
            {
                "origem": str(r.get("banco_origem") or "").strip(),
                "destino": str(r.get("banco_destino") or "").strip(),
                "valor": float(r.get("valor") or 0.0),
            }
        )
    return out
