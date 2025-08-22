"""
Repositório: Bancos Cadastrados
===============================

Opera sobre a tabela `bancos_cadastrados`:
- listar nomes cadastrados
- checar existência de um banco

Somente leitura.
"""

from typing import List
import pandas as pd
from shared.db import get_conn


class BancosCadastradosRepository:
    """Consultas simples à tabela `bancos_cadastrados`."""

    def __init__(self, caminho_banco: str):
        self.caminho_banco = caminho_banco

    def listar_nomes(self) -> List[str]:
        """Retorna todos os nomes cadastrados, ordenados."""
        with get_conn(self.caminho_banco) as conn:
            try:
                df = pd.read_sql("SELECT nome FROM bancos_cadastrados ORDER BY nome", conn)
                return df["nome"].dropna().astype(str).tolist() if not df.empty else []
            except Exception:
                return []

    def existe(self, nome: str) -> bool:
        """Retorna True se `nome` existir (case-insensitive)."""
        if not nome:
            return False
        alvo = (nome or "").strip().lower()
        return any((n or "").strip().lower() == alvo for n in self.listar_nomes())
