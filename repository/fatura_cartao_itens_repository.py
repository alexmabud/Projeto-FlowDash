# repository/fatura_cartao_itens_repository.py
from __future__ import annotations
import sqlite3
from typing import Any, Optional
from datetime import datetime
from hashlib import sha256

def _normalize_valor(v: Any) -> float:
    if v is None:
        raise ValueError("valor_parcela não pode ser None.")
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace("R$", "").replace("r$", "").strip()
    s = s.replace(".", "").replace(",", ".")
    return float(s)

def _competencia_from_data_parcela(data_compra: str, parcela_num: int) -> str:
    dt = datetime.strptime(data_compra, "%Y-%m-%d")
    y, m = dt.year, dt.month + (parcela_num - 1)
    y += (m - 1) // 12
    m = (m - 1) % 12 + 1
    return f"{y:04d}-{m:02d}"

def _det_uid_if_needed(purchase_uid: Optional[str], cartao: str,
                       data_compra: str, descricao: str, parcelas: int) -> str:
    if purchase_uid:
        return purchase_uid
    base = f"{cartao}|{data_compra}|{descricao}|{parcelas}"
    return sha256(base.encode("utf-8")).hexdigest()

class FaturaCartaoItensRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._garantir_indices()

    def _conn(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys=ON;")
        con.execute("PRAGMA busy_timeout=30000;")
        return con

    def _garantir_indices(self):
        with self._conn() as con:
            con.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_fatura_itens_uid_parc
                ON fatura_cartao_itens(purchase_uid, parcela_num);
            """)
            con.execute("""
                CREATE INDEX IF NOT EXISTS idx_fatura_itens_purchase
                ON fatura_cartao_itens(purchase_uid);
            """)
            con.execute("""
                CREATE INDEX IF NOT EXISTS idx_fatura_itens_cartao_comp
                ON fatura_cartao_itens(cartao, competencia);
            """)
            con.commit()

    def inserir_item(
        self,
        *,
        data_compra: str,
        cartao: str,
        descricao_compra: str,
        parcela_num: int = 1,
        parcelas: int = 1,
        valor_parcela: Any = None,
        categoria: Optional[str] = None,
        competencia: Optional[str] = None,
        purchase_uid: Optional[str] = None,
        usuario: Optional[str] = None,
        created_at: Optional[str] = None,
    ) -> int:
        """Insere item de fatura, idempotente por (purchase_uid, parcela_num)."""
        v = _normalize_valor(valor_parcela)
        if v <= 0:
            raise ValueError("valor_parcela deve ser > 0.")

        parcela_num = int(parcela_num or 1)
        parcelas = int(parcelas or 1)
        if parcela_num < 1 or parcelas < 1 or parcela_num > parcelas:
            raise ValueError(f"Parcela inválida: {parcela_num}/{parcelas}")

        uid = _det_uid_if_needed(purchase_uid, cartao, data_compra, descricao_compra, parcelas)
        comp = competencia or _competencia_from_data_parcela(data_compra, parcela_num)

        with self._conn() as con:
            row = con.execute(
                "SELECT id FROM fatura_cartao_itens WHERE purchase_uid=? AND parcela_num=?",
                (uid, parcela_num)
            ).fetchone()
            if row:
                return int(row["id"])

            con.execute("""
                INSERT INTO fatura_cartao_itens
                    (purchase_uid, cartao, competencia, data_compra,
                     descricao_compra, categoria,
                     parcela_num, parcelas, valor_parcela,
                     usuario, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                uid, cartao, comp, data_compra,
                descricao_compra, categoria,
                parcela_num, parcelas, float(v),
                usuario, created_at
            ))
            con.commit()
            return int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
