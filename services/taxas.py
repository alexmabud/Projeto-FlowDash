"""
Módulo TaxaMaquinetaManager
===========================

Gerencia a tabela `taxas_maquinas` no SQLite para configurar **taxas por
maquineta/PSP** em diferentes combinações de forma de pagamento, bandeira
e parcelas. Também suporta um **banco de destino** para a liquidação.
"""

from __future__ import annotations

import sqlite3
from typing import Iterable, Optional, Sequence, Tuple, Dict, Any

import pandas as pd

__all__ = ["TaxaMaquinetaManager"]


class TaxaMaquinetaManager:
    """CRUD mínimo e utilitários para a tabela `taxas_maquinas`."""

    def __init__(self, caminho_banco: str) -> None:
        self.caminho_banco = caminho_banco
        self._criar_tabela()
        self._criar_indices()

    # ------------------------------------------------------------------ #
    # Infra
    # ------------------------------------------------------------------ #
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.caminho_banco)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _criar_tabela(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS taxas_maquinas (
                    maquineta        TEXT NOT NULL,
                    forma_pagamento  TEXT NOT NULL,
                    bandeira         TEXT NOT NULL,
                    parcelas         INTEGER NOT NULL,
                    taxa_percentual  REAL NOT NULL,
                    banco_destino    TEXT,
                    PRIMARY KEY (maquineta, forma_pagamento, bandeira, parcelas)
                )
                """
            )

    def _criar_indices(self) -> None:
        with self._connect() as conn:
            # Úteis para filtros na UI
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_taxas_maquinas_maquineta ON taxas_maquinas(maquineta)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_taxas_maquinas_forma ON taxas_maquinas(forma_pagamento)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_taxas_maquinas_bandeira ON taxas_maquinas(bandeira)"
            )

    # ------------------------------------------------------------------ #
    # Validações
    # ------------------------------------------------------------------ #
    @staticmethod
    def _norm(s: Optional[str]) -> str:
        return (s or "").strip().upper()

    @staticmethod
    def _valida_parcelas(parcelas: int) -> int:
        try:
            p = int(parcelas)
        except Exception:
            raise ValueError("`parcelas` deve ser inteiro.")
        if p < 1:
            raise ValueError("`parcelas` deve ser >= 1.")
        return p

    @staticmethod
    def _valida_taxa(taxa: float) -> float:
        try:
            t = float(taxa)
        except Exception:
            raise ValueError("`taxa` deve ser número.")
        # Aceita taxas >100 para casos específicos, mas evita negativos
        if t < 0:
            raise ValueError("`taxa` não pode ser negativa.")
        return t

    # ------------------------------------------------------------------ #
    # CRUD
    # ------------------------------------------------------------------ #
    def salvar_taxa(
        self,
        maquineta: str,
        forma: str,
        bandeira: str,
        parcelas: int,
        taxa: float,
        banco_destino: Optional[str] = None,
    ) -> None:
        """Insere ou atualiza uma taxa (INSERT OR REPLACE)."""
        maq = self._norm(maquineta)
        frm = self._norm(forma)
        ban = self._norm(bandeira)
        par = self._valida_parcelas(parcelas)
        tx = self._valida_taxa(taxa)
        bco = (banco_destino or "").strip()

        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO taxas_maquinas
                    (maquineta, forma_pagamento, bandeira, parcelas, taxa_percentual, banco_destino)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (maq, frm, ban, par, tx, bco),
            )
            conn.commit()

    def salvar_taxas_bulk(
        self,
        itens: Iterable[Tuple[str, str, str, int, float, Optional[str]]],
    ) -> None:
        """
        Insere/atualiza várias taxas.
        `itens` = iterável de (maquineta, forma, bandeira, parcelas, taxa, banco_destino)
        """
        rows = []
        for maquineta, forma, bandeira, parcelas, taxa, banco_destino in itens:
            rows.append(
                (
                    self._norm(maquineta),
                    self._norm(forma),
                    self._norm(bandeira),
                    self._valida_parcelas(parcelas),
                    self._valida_taxa(taxa),
                    (banco_destino or "").strip(),
                )
            )

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO taxas_maquinas
                    (maquineta, forma_pagamento, bandeira, parcelas, taxa_percentual, banco_destino)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

    def remover_taxa(
        self,
        maquineta: str,
        forma: str,
        bandeira: str,
        parcelas: int,
    ) -> int:
        """Remove uma taxa específica. Retorna o número de linhas afetadas."""
        maq = self._norm(maquineta)
        frm = self._norm(forma)
        ban = self._norm(bandeira)
        par = self._valida_parcelas(parcelas)

        with self._connect() as conn:
            cur = conn.execute(
                """
                DELETE FROM taxas_maquinas
                 WHERE maquineta=? AND forma_pagamento=? AND bandeira=? AND parcelas=?
                """,
                (maq, frm, ban, par),
            )
            conn.commit()
            return cur.rowcount

    def obter_taxa(
        self,
        maquineta: str,
        forma: str,
        bandeira: str,
        parcelas: int,
    ) -> Optional[Dict[str, Any]]:
        """Retorna um dict com os campos da taxa ou None."""
        maq = self._norm(maquineta)
        frm = self._norm(forma)
        ban = self._norm(bandeira)
        par = self._valida_parcelas(parcelas)

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT maquineta, forma_pagamento, bandeira, parcelas, taxa_percentual, COALESCE(banco_destino,'')
                  FROM taxas_maquinas
                 WHERE maquineta=? AND forma_pagamento=? AND bandeira=? AND parcelas=?
                 LIMIT 1
                """,
                (maq, frm, ban, par),
            ).fetchone()

        if not row:
            return None

        return {
            "maquineta": row[0],
            "forma_pagamento": row[1],
            "bandeira": row[2],
            "parcelas": int(row[3]),
            "taxa_percentual": float(row[4]),
            "banco_destino": row[5],
        }

    # ------------------------------------------------------------------ #
    # Leitura para UI
    # ------------------------------------------------------------------ #
    def carregar_taxas(
        self,
        *,
        maquineta: Optional[str] = None,
        forma: Optional[str] = None,
        bandeira: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Carrega as taxas em um DataFrame com colunas amigáveis.
        Filtros são opcionais e aceitam `None`.
        """
        filtros = []
        params: list = []

        if maquineta:
            filtros.append("UPPER(maquineta) = ?")
            params.append(self._norm(maquineta))
        if forma:
            filtros.append("UPPER(forma_pagamento) = ?")
            params.append(self._norm(forma))
        if bandeira:
            filtros.append("UPPER(bandeira) = ?")
            params.append(self._norm(bandeira))

        where = ""
        if filtros:
            where = "WHERE " + " AND ".join(filtros)

        sql = f"""
            SELECT
                UPPER(maquineta)        AS "Maquineta",
                UPPER(forma_pagamento)  AS "Forma de Pagamento",
                UPPER(bandeira)         AS "Bandeira",
                parcelas                AS "Parcelas",
                taxa_percentual         AS "Taxa (%)",
                COALESCE(banco_destino, '') AS "Banco Destino"
            FROM taxas_maquinas
            {where}
            ORDER BY maquineta, forma_pagamento, bandeira, parcelas
        """

        with self._connect() as conn:
            df = pd.read_sql(sql, conn, params=params)

        # Tipagens amigáveis
        if not df.empty:
            df["Parcelas"] = df["Parcelas"].astype(int)
            df["Taxa (%)"] = df["Taxa (%)"].astype(float)

        return df
