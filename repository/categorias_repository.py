"""
Módulo CategoriasRepository
===========================

Este módulo define a classe `CategoriasRepository`, responsável por gerenciar
as tabelas de **categorias** e **subcategorias** no banco de dados SQLite.
Ele centraliza operações de criação, consulta e manutenção da hierarquia de
classificação das movimentações financeiras.

Funcionalidades principais
--------------------------
- Criação automática do schema das tabelas `categorias` e `subcategorias`.
- Cadastro, alteração e exclusão de categorias e subcategorias.
- Consulta de categorias e subcategorias ativas.
- Relacionamento entre categoria → subcategoria (chave estrangeira).
- Suporte a filtragem e ordenação para uso em formulários de entrada e saída.

Detalhes técnicos
-----------------
- Conexão SQLite configurada em modo WAL, com busy_timeout e suporte a
  foreign keys.
- Garantia de integridade referencial entre categorias e subcategorias.
- Organização pensada para integração direta com o `LedgerService` e
  páginas de lançamentos (entradas, saídas, boletos e cartões).

Dependências
------------
- sqlite3
- pandas
- typing (Optional, List, Dict)

"""

import sqlite3
import pandas as pd
from typing import Optional, List, Tuple

class CategoriasRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_schema()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _ensure_schema(self):
        with self._get_conn() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS categorias_saida (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT UNIQUE NOT NULL
            );
            CREATE TABLE IF NOT EXISTS subcategorias_saida (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                categoria_id INTEGER NOT NULL,
                nome         TEXT NOT NULL,
                UNIQUE(categoria_id, nome),
                FOREIGN KEY(categoria_id) REFERENCES categorias_saida(id) ON DELETE CASCADE
            );
            """)
            conn.commit()

    # ------------- categorias -------------
    def listar_categorias(self) -> pd.DataFrame:
        with self._get_conn() as conn:
            return pd.read_sql("SELECT id, nome FROM categorias_saida ORDER BY nome", conn)

    def adicionar_categoria(self, nome: str) -> Optional[int]:
        nome = (nome or "").strip()
        if not nome:
            return None
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO categorias_saida (nome) VALUES (?)", (nome,))
            conn.commit()
            # retorna id (busca mesmo se já existia)
            row = conn.execute("SELECT id FROM categorias_saida WHERE nome = ? LIMIT 1", (nome,)).fetchone()
            return row[0] if row else None

    def excluir_categoria(self, categoria_id: int) -> None:
        with self._get_conn() as conn:
            conn.execute("DELETE FROM categorias_saida WHERE id = ?", (categoria_id,))
            conn.commit()

    def obter_categoria_por_nome(self, nome: str) -> Optional[Tuple[int, str]]:
        with self._get_conn() as conn:
            row = conn.execute("SELECT id, nome FROM categorias_saida WHERE nome = ? LIMIT 1", (nome,)).fetchone()
            return (row[0], row[1]) if row else None

    # ------------- subcategorias -------------
    def listar_subcategorias(self, categoria_id: int) -> pd.DataFrame:
        with self._get_conn() as conn:
            return pd.read_sql(
                "SELECT id, nome FROM subcategorias_saida WHERE categoria_id = ? ORDER BY nome",
                conn, params=(categoria_id,)
            )

    def adicionar_subcategoria(self, categoria_id: int, nome: str) -> Optional[int]:
        nome = (nome or "").strip()
        if not nome:
            return None
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT OR IGNORE INTO subcategorias_saida (categoria_id, nome)
                VALUES (?, ?)
            """, (int(categoria_id), nome))
            conn.commit()
            row = conn.execute("""
                SELECT id FROM subcategorias_saida
                WHERE categoria_id = ? AND nome = ? LIMIT 1
            """, (int(categoria_id), nome)).fetchone()
            return row[0] if row else None

    def excluir_subcategoria(self, subcat_id: int) -> None:
        with self._get_conn() as conn:
            conn.execute("DELETE FROM subcategorias_saida WHERE id = ?", (subcat_id,))
            conn.commit()

    def obter_sub_por_nome(self, categoria_id: int, nome: str) -> Optional[Tuple[int, str]]:
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT id, nome FROM subcategorias_saida
                WHERE categoria_id = ? AND nome = ? LIMIT 1
            """, (int(categoria_id), nome)).fetchone()
            return (row[0], row[1]) if row else None