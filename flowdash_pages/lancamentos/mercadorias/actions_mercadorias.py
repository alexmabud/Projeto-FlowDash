# ===================== Actions: Mercadorias =====================
"""
Ações com acesso ao banco (SQLite) – mesma lógica do módulo original.
- Compra: INSERT em `mercadorias`
- Recebimento: UPDATE em `mercadorias` (garante colunas extras)
"""

from __future__ import annotations

import pandas as pd
from shared.db import get_conn

# ---- helpers DB (mesmos do original)
def _to_float_or_none(x):
    if x is None:
        return None
    try:
        s = str(x).strip().replace(",", ".")
        return float(s) if s != "" else None
    except Exception:
        return None

def _ensure_extra_cols(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(mercadorias);")
    cols = {r[1] for r in cur.fetchall()}
    to_add = []
    if "Valor_Recebido" not in cols:
        to_add.append('ALTER TABLE mercadorias ADD COLUMN Valor_Recebido REAL;')
    if "Frete_Cobrado" not in cols:
        to_add.append('ALTER TABLE mercadorias ADD COLUMN Frete_Cobrado REAL;')
    if "Recebimento_Obs" not in cols:
        to_add.append('ALTER TABLE mercadorias ADD COLUMN Recebimento_Obs TEXT;')
    for sql in to_add:
        cur.execute(sql)
    if to_add:
        conn.commit()

# ---- Compra
def salvar_compra(caminho_banco: str, payload: dict) -> str:
    """
    Insere compra de mercadorias. Mantém validações e conversões do original.
    Retorna mensagem de sucesso.
    """
    data_txt = payload["data_txt"]
    colecao = payload["colecao"]
    fornecedor = payload["fornecedor"]
    valor_mercadoria = float(payload["valor_mercadoria"] or 0.0)
    frete_f = _to_float_or_none(payload.get("frete"))
    forma_pagamento = (payload["forma_pagamento"] or "").strip().upper()
    parcelas_int = int(payload.get("parcelas") or 1)
    prev_fat = payload.get("prev_fat_dt")
    prev_rec = payload.get("prev_rec_dt")
    numero_pedido = _to_float_or_none(payload.get("numero_pedido"))
    numero_nf = _to_float_or_none(payload.get("numero_nf"))

    if not fornecedor or valor_mercadoria <= 0:
        raise ValueError("Informe fornecedor e um valor de mercadoria maior que zero.")
    if forma_pagamento == "CRÉDITO" and parcelas_int < 1:
        raise ValueError("Em CRÉDITO, defina Parcelas ≥ 1.")
    if parcelas_int < 1:
        parcelas_int = 1

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO mercadorias (
                Data, Colecao, Fornecedor, Valor_Mercadoria, Frete,
                Forma_Pagamento, Parcelas,
                Previsao_Faturamento, Faturamento,
                Previsao_Recebimento, Recebimento,
                Numero_Pedido, Numero_NF
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data_txt, colecao, fornecedor, float(valor_mercadoria), frete_f,
            forma_pagamento, int(parcelas_int),
            prev_fat, None,
            prev_rec, None,
            numero_pedido, numero_nf
        ))
        conn.commit()

    return "✅ Compra registrada com sucesso!"

# ---- Recebimento
def carregar_compras(caminho_banco: str, incluir_recebidas: bool = False) -> list[dict]:
    """
    Carrega compras (pendentes por padrão). Limite 200 como no original.
    """
    with get_conn(caminho_banco) as conn:
        _ensure_extra_cols(conn)
        cur = conn.cursor()

        base_select = """
            SELECT id, Data, Colecao, Fornecedor,
                   Previsao_Faturamento, Previsao_Recebimento, Numero_Pedido,
                   Recebimento,
                   Valor_Mercadoria, Frete,
                   Numero_NF
              FROM mercadorias
        """
        where_clause = "" if incluir_recebidas else "WHERE Recebimento IS NULL OR TRIM(Recebimento) = ''"

        rows = cur.execute(f"""
            {base_select}
            {where_clause}
            ORDER BY date(Data) DESC, rowid DESC
            LIMIT 200
        """).fetchall()

        compras = [
            {
                "id": r[0],
                "Data": r[1] or "",
                "Colecao": r[2] or "",
                "Fornecedor": r[3] or "",
                "PrevFat": r[4] or "",
                "PrevRec": r[5] or "",
                "Pedido": r[6],
                "Recebimento": r[7],
                "Valor_Mercadoria": float(r[8]) if r[8] is not None else 0.0,
                "Frete": float(r[9]) if r[9] is not None else 0.0,
                "Numero_Pedido": "" if r[6] is None else str(r[6]),
                "Numero_NF": "" if r[10] is None else str(r[10]),
            } for r in rows
        ]
        return compras

def salvar_recebimento(caminho_banco: str, payload: dict) -> str:
    """
    Atualiza recebimento/ajustes da compra selecionada. Mantém SQL do original.
    """
    sel_id = int(payload["selected_id"])
    with get_conn(caminho_banco) as conn:
        _ensure_extra_cols(conn)
        cur = conn.cursor()
        cur.execute("""
            UPDATE mercadorias
               SET Faturamento = ?,
                   Recebimento = ?,
                   Valor_Recebido = ?,
                   Frete_Cobrado = ?,
                   Recebimento_Obs = ?,
                   Numero_Pedido = ?,
                   Numero_NF = ?
             WHERE id = ?
        """, (
            payload.get("fat_dt"),
            payload.get("rec_dt"),
            _to_float_or_none(payload.get("valor_recebido")),
            _to_float_or_none(payload.get("frete_cobrado")),
            payload.get("obs"),
            payload.get("numero_pedido"),
            payload.get("numero_nf"),
            sel_id
        ))
        conn.commit()

    return "✅ Recebimento registrado/atualizado com sucesso!"
