"""
Módulo Base (Contas a Pagar - Repositório)
==========================================

Este módulo define a classe `BaseRepo`, que fornece **funcionalidades comuns**
aos mixins do repositório `contas_a_pagar_mov_repository`.

Funcionalidades principais
--------------------------
- Abrir conexão SQLite com PRAGMAs padronizados:
  - `journal_mode=WAL`
  - `busy_timeout=30000`
  - `foreign_keys=ON`
- Validação de eventos antes da inserção.
- Inserção genérica de eventos em `contas_a_pagar_mov`.
- Recuperação do próximo `obrigacao_id` sequencial.

Detalhes técnicos
-----------------
- Reaproveitado por todos os mixins (`EventsMixin`, `PaymentsMixin`, etc.).
- Validações usam listas de valores permitidos de `types.py`.

Dependências
------------
- sqlite3
- repository.contas_a_pagar_mov_repository.types (ALLOWED_TIPOS, ALLOWED_CATEGORIAS)
"""

import sqlite3
from typing import Optional

from repository.contas_a_pagar_mov_repository.types import ALLOWED_TIPOS, ALLOWED_CATEGORIAS


class BaseRepo(object):
    """Classe base com utilitários comuns para os mixins do repositório de Contas a Pagar."""

    def __init__(self, db_path: str, *args, **kwargs):
        # __init__ cooperativo para múltipla herança com mixins
        super().__init__(*args, **kwargs)
        self.db_path = db_path

    # ------------------ conexão / PRAGMAs ------------------

    def _get_conn(self) -> sqlite3.Connection:
        """
        Abre conexão SQLite com PRAGMAs padronizados do projeto:
        - WAL
        - busy_timeout=30000
        - foreign_keys=ON
        """
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    # ------------------ helpers internos ------------------

    def _validar_evento_basico(
        self,
        *,
        obrigacao_id: int,
        tipo_obrigacao: str,
        categoria_evento: str,
        data_evento: str,
        valor_evento: float,
        usuario: str,
    ) -> None:
        """Valida campos básicos de um evento antes da inserção no banco."""
        if not isinstance(obrigacao_id, int):
            raise ValueError("obrigacao_id deve ser int.")
        if tipo_obrigacao not in ALLOWED_TIPOS:
            raise ValueError(f"tipo_obrigacao inválido: {tipo_obrigacao}. Use {sorted(ALLOWED_TIPOS)}")
        if categoria_evento not in ALLOWED_CATEGORIAS:
            raise ValueError(f"categoria_evento inválida: {categoria_evento}. Use {sorted(ALLOWED_CATEGORIAS)}")
        if not data_evento or len(data_evento) < 8:
            raise ValueError("data_evento deve ser 'YYYY-MM-DD'.")
        if float(valor_evento) == 0:
            raise ValueError("valor_evento deve ser diferente de zero.")
        if not usuario:
            raise ValueError("usuario é obrigatório.")

    def _inserir_evento(self, conn: sqlite3.Connection, **ev) -> int:
        """
        Insere um evento na tabela central `contas_a_pagar_mov`.
        Espera que os campos já tenham sido validados.
        Preenche colunas opcionais com None quando não informadas.
        """
        cols = [
            "obrigacao_id", "tipo_obrigacao", "categoria_evento", "data_evento", "vencimento",
            "valor_evento", "descricao", "credor", "competencia", "parcela_num", "parcelas_total",
            "forma_pagamento", "origem", "ledger_id", "usuario"
        ]
        sql = f"INSERT INTO contas_a_pagar_mov ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
        cur = conn.cursor()
        cur.execute(sql, [ev.get(c) for c in cols])
        return int(cur.lastrowid)

    def proximo_obrigacao_id(self, conn: sqlite3.Connection) -> int:
        """
        Retorna o próximo `obrigacao_id` sequencial
        (máximo atual + 1, ou 1 se tabela vazia).
        """
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(obrigacao_id), 0) + 1 FROM contas_a_pagar_mov;")
        return int(cur.fetchone()[0])


# API pública explícita
__all__ = ["BaseRepo"]
