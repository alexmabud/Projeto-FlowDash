# flowdash_pages/lancamentos/mercadorias/actions_mercadorias.py
"""Ações de negócio para Mercadorias.

Somente funções chamadas pela página:
- salvar_compra
- carregar_compras
- salvar_recebimento

Observação:
    Este módulo não renderiza UI (evita import circular com a página).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Final
import sqlite3

from shared.db import get_conn  # helper de conexão do projeto

__all__ = ["salvar_compra", "carregar_compras", "salvar_recebimento"]

# ---------------- Constantes & Schema ----------------
TBL_MERCADORIAS: Final[str] = "mercadorias"

# Colunas padronizadas da tabela `mercadorias`
_COLS: Final[List[tuple[str, str]]] = [
    ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("Data", "TEXT"),
    ("Colecao", "TEXT"),
    ("Fornecedor", "TEXT"),
    ("Valor_Mercadoria", "REAL"),
    ("Frete", "REAL"),
    ("Forma_Pagamento", "TEXT"),
    ("Parcelas", "INTEGER"),
    ("Previsao_Faturamento", "TEXT"),
    ("Previsao_Recebimento", "TEXT"),
    # Preenchidas apenas no recebimento:
    ("Faturamento", "TEXT"),
    ("Recebimento", "TEXT"),
    ("Valor_Recebido", "REAL"),
    ("Frete_Cobrado", "REAL"),
    ("Recebimento_Obs", "TEXT"),
    ("Numero_Pedido", "TEXT"),
    ("Numero_NF", "TEXT"),
]


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Cria a tabela `mercadorias` (se faltar) e garante suas colunas.

    A operação é idempotente: repetir não causa erro.
    """
    conn.execute(f'CREATE TABLE IF NOT EXISTS {TBL_MERCADORIAS} (id INTEGER PRIMARY KEY AUTOINCREMENT);')

    # Colunas atuais da tabela
    cur = conn.execute(f"PRAGMA table_info({TBL_MERCADORIAS});")
    cols = {row["name"] if isinstance(row, dict) else row[1] for row in cur.fetchall()}

    # Adiciona colunas que faltarem
    for col, ctype in _COLS:
        if col not in cols:
            conn.execute(f'ALTER TABLE {TBL_MERCADORIAS} ADD COLUMN "{col}" {ctype};')


# ---------------- Utilitários ----------------
def _coerce_str(v: Any) -> Optional[str]:
    """Converte para str normalizada (strip). Retorna None quando vazio/None."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None


def _coerce_float(v: Any) -> Optional[float]:
    """Converte para float. Aceita strings com vírgula. Retorna None quando vazio/inválido."""
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.replace(",", ".").strip()
            if v == "":
                return None
        return float(v)
    except Exception:
        return None


def _coerce_int(v: Any) -> Optional[int]:
    """Converte para int, retornando None quando não aplicável."""
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


# ---------------- Ações públicas ----------------
def salvar_compra(caminho_banco: str, payload: Dict[str, Any]) -> str:
    """Grava uma **compra** na tabela `mercadorias`.

    Apenas campos de compra são aceitos; campos de recebimento são ignorados/zerados.

    Args:
        caminho_banco: Caminho do arquivo SQLite.
        payload: Dados vindos do formulário de compra.

    Returns:
        Mensagem de sucesso.
    """
    # Whitelist de compra (evita colunas indevidas)
    campos_ok = {
        "data_txt",
        "colecao",
        "fornecedor",
        "valor_mercadoria",
        "frete",
        "forma_pagamento",
        "parcelas",
        "prev_fat_dt",
        "prev_rec_dt",
        "numero_pedido",
        "numero_nf",
    }
    data = {k: payload.get(k) for k in campos_ok}

    with get_conn(caminho_banco) as conn:
        conn.row_factory = sqlite3.Row
        _ensure_schema(conn)

        row = {
            "Data": _coerce_str(data.get("data_txt")),
            "Colecao": _coerce_str(data.get("colecao")),
            "Fornecedor": _coerce_str(data.get("fornecedor")),
            "Valor_Mercadoria": _coerce_float(data.get("valor_mercadoria")) or 0.0,
            "Frete": _coerce_float(data.get("frete")),
            "Forma_Pagamento": _coerce_str(data.get("forma_pagamento")),
            "Parcelas": _coerce_int(data.get("parcelas")) or 1,
            "Previsao_Faturamento": _coerce_str(data.get("prev_fat_dt")),
            "Previsao_Recebimento": _coerce_str(data.get("prev_rec_dt")),
            "Numero_Pedido": _coerce_str(data.get("numero_pedido")),
            "Numero_NF": _coerce_str(data.get("numero_nf")),
            # Campos de recebimento ficam nulos aqui
            "Faturamento": None,
            "Recebimento": None,
            "Valor_Recebido": None,
            "Frete_Cobrado": None,
            "Recebimento_Obs": None,
        }

        cols = ", ".join([f'"{c}"' for c in row.keys()])
        qs = ", ".join(["?"] * len(row))
        conn.execute(f"INSERT INTO {TBL_MERCADORIAS} ({cols}) VALUES ({qs});", list(row.values()))

    return "Compra registrada com sucesso."


def carregar_compras(caminho_banco: str, incluir_recebidas: bool = False) -> List[Dict[str, Any]]:
    """Carrega compras para exibição na UI.

    Quando `incluir_recebidas=False`, retorna apenas as não recebidas (`Recebimento IS NULL`).

    Args:
        caminho_banco: Caminho do arquivo SQLite.
        incluir_recebidas: Se True, retorna todas as compras.

    Returns:
        Lista de dicionários com os campos esperados pela página/form.
    """
    with get_conn(caminho_banco) as conn:
        conn.row_factory = sqlite3.Row
        _ensure_schema(conn)

        base_sql = f"""
            SELECT
                id,
                Data,
                Colecao,
                Fornecedor,
                Previsao_Faturamento,
                Previsao_Recebimento,
                Valor_Mercadoria,
                Frete,
                Numero_Pedido,
                Numero_NF,
                Recebimento
            FROM {TBL_MERCADORIAS}
        """
        if not incluir_recebidas:
            base_sql += " WHERE Recebimento IS NULL"

        base_sql += " ORDER BY COALESCE(Data, Previsao_Recebimento) ASC, id ASC;"

        rows = conn.execute(base_sql).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            out.append(
                {
                    "id": d.get("id"),
                    "Data": d.get("Data"),
                    "Colecao": d.get("Colecao"),
                    "Fornecedor": d.get("Fornecedor"),
                    # aliases esperados pelo form de recebimento
                    "PrevFat": d.get("Previsao_Faturamento"),
                    "PrevRec": d.get("Previsao_Recebimento"),
                    "Valor_Mercadoria": d.get("Valor_Mercadoria"),
                    "Frete": d.get("Frete"),
                    "Numero_Pedido": d.get("Numero_Pedido"),
                    "Numero_NF": d.get("Numero_NF"),
                    "Recebimento": d.get("Recebimento"),
                }
            )
        return out


def salvar_recebimento(caminho_banco: str, payload: Dict[str, Any]) -> str:
    """Atualiza uma linha de `mercadorias` com dados de **recebimento**.

    Args:
        caminho_banco: Caminho do arquivo SQLite.
        payload: Dados vindos do formulário de recebimento.

    Returns:
        Mensagem de sucesso.

    Raises:
        ValueError: Quando `selected_id` está ausente ou inválido.
    """
    if not payload or "selected_id" not in payload:
        raise ValueError("Payload do recebimento inválido: ID ausente.")

    rid = _coerce_int(payload.get("selected_id"))
    if not rid:
        raise ValueError("ID selecionado inválido.")

    with get_conn(caminho_banco) as conn:
        conn.row_factory = sqlite3.Row
        _ensure_schema(conn)

        # Apenas campos de recebimento (e os dois editáveis)
        updates = {
            "Faturamento": _coerce_str(payload.get("fat_dt")),
            "Recebimento": _coerce_str(payload.get("rec_dt")),
            "Valor_Recebido": _coerce_float(payload.get("valor_recebido")),
            "Frete_Cobrado": _coerce_float(payload.get("frete_cobrado")),
            "Recebimento_Obs": _coerce_str(payload.get("obs")),
            "Numero_Pedido": _coerce_str(payload.get("numero_pedido")),
            "Numero_NF": _coerce_str(payload.get("numero_nf")),
        }

        sets = ", ".join([f'"{k}" = ?' for k in updates.keys()])
        vals = list(updates.values()) + [rid]

        conn.execute(f'UPDATE {TBL_MERCADORIAS} SET {sets} WHERE id = ?;', vals)

    return "Recebimento registrado com sucesso."
