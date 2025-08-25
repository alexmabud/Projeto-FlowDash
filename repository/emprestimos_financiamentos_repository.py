"""
Módulo Empréstimos e Financiamentos (Repositório)
=================================================

Este módulo define a classe `EmprestimosFinanciamentosRepository`, responsável
por consultas à tabela **`emprestimos_financiamentos`** no SQLite. Fornece
operações de leitura para alimentar selects da UI de pagamentos (saídas).

Funcionalidades principais
--------------------------
- Listagem de rótulos distintos (banco/descrição/tipo).
- Normalização (TRIM, COALESCE) para garantir consistência visual na UI.
- Retorno ordenado e sem duplicações.

Detalhes técnicos
-----------------
- Conexão SQLite via helper `get_conn` (shared.db).
- Operação somente leitura.
- Resiliente a falhas: retorna lista vazia se tabela não existir.

Dependências
------------
- pandas
- typing (List)
- shared.db.get_conn
"""

from typing import List
import pandas as pd

from shared.db import get_conn


class EmprestimosFinanciamentosRepository:
    """Consultas utilitárias à tabela `emprestimos_financiamentos`."""

    def __init__(self, caminho_banco: str):
        self.caminho_banco = caminho_banco

    def listar_rotulos(self) -> List[str]:
        """
        Retorna rótulos distintos (banco/descrição/tipo), já limpos e ordenados,
        para popular selects na UI.

        Retorno:
            list[str]: Lista de rótulos únicos, ordenados alfabeticamente.
        """
        sql = """
            SELECT DISTINCT
                   TRIM(
                       COALESCE(
                           NULLIF(TRIM(banco), ''),
                           NULLIF(TRIM(descricao), ''),
                           NULLIF(TRIM(tipo), '')
                       )
                   ) AS rotulo
              FROM emprestimos_financiamentos
        """
        with get_conn(self.caminho_banco) as conn:
            try:
                df = pd.read_sql(sql, conn)
            except Exception:
                return []
        if df.empty:
            return []
        df = df.dropna()
        df = df[df["rotulo"] != ""]
        return df["rotulo"].drop_duplicates().sort_values().tolist()


# API pública explícita
__all__ = ["EmprestimosFinanciamentosRepository"]
