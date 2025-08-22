"""
Repositório: Empréstimos e Financiamentos
=========================================

Consultas para montar listas na UI de Pagamentos (Saída):
- listar rótulos possíveis (banco/descrição/tipo) para empréstimos/financiamentos

Somente leitura.
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
        """
        sql = """
            SELECT DISTINCT
                   TRIM(
                       COALESCE(
                           NULLIF(TRIM(banco),''),
                           NULLIF(TRIM(descricao),''),
                           NULLIF(TRIM(tipo),'')
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
