"""
Módulo Bancos Cadastrados (Repositório)
=======================================

Este módulo define a classe `BancosCadastradosRepository`, responsável por
consultar a tabela **`bancos_cadastrados`** no SQLite. Fornece operações
simples de leitura para uso em formulários e validações do sistema.

Funcionalidades principais
--------------------------
- Listagem de nomes cadastrados (ordenados).
- Checagem de existência de banco por nome (case-insensitive).

Detalhes técnicos
-----------------
- Conexão SQLite via helper `get_conn` (shared.db).
- Operações seguras contra falhas: se a tabela não existir, retorna lista vazia.
- Case-insensitive para verificar existência.

Dependências
------------
- pandas
- typing (List)
- shared.db.get_conn
"""

from typing import List
import pandas as pd

from shared.db import get_conn


class BancosCadastradosRepository:
    """Consultas simples à tabela `bancos_cadastrados`."""

    def __init__(self, caminho_banco: str):
        self.caminho_banco = caminho_banco

    def listar_nomes(self) -> List[str]:
        """
        Retorna todos os nomes de bancos cadastrados, ordenados alfabeticamente.

        Retorno:
            list[str]: Lista de nomes ou lista vazia se tabela não existir.
        """
        with get_conn(self.caminho_banco) as conn:
            try:
                df = pd.read_sql("SELECT nome FROM bancos_cadastrados ORDER BY nome", conn)
                return df["nome"].dropna().astype(str).tolist() if not df.empty else []
            except Exception:
                return []

    def existe(self, nome: str) -> bool:
        """
        Verifica se `nome` já está cadastrado na tabela.

        Parâmetros:
            nome (str): Nome do banco a verificar.

        Retorno:
            bool: True se o nome existir (case-insensitive), False caso contrário.
        """
        if not nome:
            return False
        alvo = (nome or "").strip().lower()
        return any((n or "").strip().lower() == alvo for n in self.listar_nomes())


# API pública explícita
__all__ = ["BancosCadastradosRepository"]
