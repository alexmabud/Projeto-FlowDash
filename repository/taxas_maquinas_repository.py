"""
Módulo Taxas por Maquineta (Repositório)
========================================

Este módulo define a classe `TaxasMaquinasRepository`, responsável por consultas
à tabela **`taxas_maquinas`** no SQLite. Fornece operações de leitura para
alimentar selects na UI e para cálculo de liquidação líquida.

Funcionalidades principais
--------------------------
- Listagem de maquinetas disponíveis por forma de pagamento.
- Listagem de bandeiras por (forma, maquineta).
- Listagem de parcelas por (forma, maquineta, bandeira).
- Consulta de taxa (%) e banco de destino da liquidação.
- Heurística de fallback para descobrir `banco_destino`.

Detalhes técnicos
-----------------
- Operações somente leitura (não altera dados).
- Conexão SQLite via helper `get_conn` (shared.db).
- Comparações **case-insensitive** para `forma_pagamento` usando UPPER.
- Retorno resiliente: listas vazias ou `None` em caso de falha.

Dependências
------------
- pandas
- typing (Iterable, List, Optional, Tuple)
- shared.db.get_conn
"""

from typing import Iterable, List, Optional, Tuple
import pandas as pd

from shared.db import get_conn


class TaxasMaquinasRepository:
    """Consultas utilitárias sobre `taxas_maquinas`."""

    def __init__(self, caminho_banco: str):
        self.caminho_banco = caminho_banco

    # ---------- listas para UI ----------

    def listar_maquinetas_por_forma(self, formas: Iterable[str]) -> List[str]:
        """Retorna maquinetas distintas para as `formas` informadas."""
        formas_u = [str(f).upper() for f in formas or []]
        if not formas_u:
            return []
        placeholders = ",".join(["?"] * len(formas_u))
        with get_conn(self.caminho_banco) as conn:
            df = pd.read_sql(
                f"""
                SELECT DISTINCT maquineta
                  FROM taxas_maquinas
                 WHERE UPPER(forma_pagamento) IN ({placeholders})
                 ORDER BY maquineta
                """,
                conn,
                params=formas_u,
            )
        return df["maquineta"].dropna().astype(str).tolist() if not df.empty else []

    def listar_bandeiras(self, formas: Iterable[str], maquineta: str) -> List[str]:
        """Retorna bandeiras distintas para (formas, maquineta)."""
        formas_u = [str(f).upper() for f in formas or []]
        if not formas_u or not maquineta:
            return []
        placeholders = ",".join(["?"] * len(formas_u))
        with get_conn(self.caminho_banco) as conn:
            df = pd.read_sql(
                f"""
                SELECT DISTINCT bandeira
                  FROM taxas_maquinas
                 WHERE UPPER(forma_pagamento) IN ({placeholders})
                   AND maquineta = ?
                 ORDER BY bandeira
                """,
                conn,
                params=formas_u + [maquineta],
            )
        return df["bandeira"].dropna().astype(str).tolist() if not df.empty else []

    def listar_parcelas(self, formas: Iterable[str], maquineta: str, bandeira: str) -> List[int]:
        """Retorna parcelas distintas para (formas, maquineta, bandeira)."""
        formas_u = [str(f).upper() for f in formas or []]
        if not formas_u or not maquineta or bandeira is None:
            return []
        placeholders = ",".join(["?"] * len(formas_u))
        with get_conn(self.caminho_banco) as conn:
            df = pd.read_sql(
                f"""
                SELECT DISTINCT parcelas
                  FROM taxas_maquinas
                 WHERE UPPER(forma_pagamento) IN ({placeholders})
                   AND maquineta = ?
                   AND bandeira = ?
                 ORDER BY parcelas
                """,
                conn,
                params=formas_u + [maquineta, bandeira],
            )
        return df["parcelas"].dropna().astype(int).tolist() if not df.empty else []

    # ---------- taxa + banco_destino ----------

    def obter_taxa_e_banco_destino(
        self,
        forma: str,
        maquineta: str,
        bandeira: Optional[str],
        parcelas: Optional[int],
    ) -> Tuple[float, Optional[str]]:
        """
        Retorna (taxa_percentual, banco_destino) para um registro exato.

        Caso não exista, retorna (0.0, None).
        """
        if not forma or not maquineta:
            return 0.0, None
        forma_u = forma.upper()
        with get_conn(self.caminho_banco) as conn:
            row = conn.execute(
                """
                SELECT taxa_percentual, banco_destino
                  FROM taxas_maquinas
                 WHERE UPPER(forma_pagamento)=?
                   AND maquineta=?
                   AND bandeira=?
                   AND parcelas=?
                 LIMIT 1
                """,
                (forma_u, maquineta, bandeira or "", int(parcelas or 1)),
            ).fetchone()
        if not row:
            return 0.0, None
        return float(row[0] or 0.0), (row[1] or None)

    def descobrir_banco_destino(
        self,
        forma: str,
        maquineta: str,
        bandeira: Optional[str],
        parcelas: Optional[int],
    ) -> Optional[str]:
        """
        Heurística de fallback para `banco_destino` (espelha a lógica atual da UI):

        1) match exato por (forma, maquineta, bandeira, parcelas)
        2) match por (forma, maquineta) ignorando bandeira/parcelas
        3) qualquer registro da maquineta com banco_destino definido
        4) se `forma == LINK_PAGAMENTO`, tenta novamente como `CRÉDITO`
        """
        if not forma or not maquineta:
            return None

        formas_try = [forma.upper()]
        if forma.upper() == "LINK_PAGAMENTO":
            formas_try.append("CRÉDITO")

        with get_conn(self.caminho_banco) as conn:
            # 1) exato
            for f in formas_try:
                row = conn.execute(
                    """
                    SELECT banco_destino FROM taxas_maquinas
                     WHERE UPPER(forma_pagamento)=?
                       AND maquineta=?
                       AND bandeira=?
                       AND parcelas=?
                     LIMIT 1
                    """,
                    (f, maquineta, bandeira or "", int(parcelas or 1)),
                ).fetchone()
                if row and row[0]:
                    return row[0]

            # 2) por maquineta
            for f in formas_try:
                row = conn.execute(
                    """
                    SELECT banco_destino FROM taxas_maquinas
                     WHERE UPPER(forma_pagamento)=?
                       AND maquineta=?
                       AND banco_destino IS NOT NULL AND TRIM(banco_destino)<>''
                     LIMIT 1
                    """,
                    (f, maquineta),
                ).fetchone()
                if row and row[0]:
                    return row[0]

            # 3) qualquer da maquineta
            row = conn.execute(
                """
                SELECT banco_destino FROM taxas_maquinas
                 WHERE maquineta=?
                   AND banco_destino IS NOT NULL AND TRIM(banco_destino)<>''
                 LIMIT 1
                """,
                (maquineta,),
            ).fetchone()
            if row and row[0]:
                return row[0]

        return None


# API pública explícita
__all__ = ["TaxasMaquinasRepository"]
