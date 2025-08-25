"""
Módulo Categorias (Repositório)
===============================

Este módulo define a classe `CategoriasRepository`, responsável por acessar e
gerenciar as tabelas **`categorias_saida`** e **`subcategorias_saida`** no SQLite.
Centraliza operações de criação de schema (idempotente), cadastro e consulta da
hierarquia de categorias → subcategorias usada nos lançamentos.

Funcionalidades principais
--------------------------
- Criação automática do schema (`categorias_saida`, `subcategorias_saida`).
- Cadastro, listagem e exclusão de categorias e subcategorias.
- Relacionamento via chave estrangeira (subcategoria → categoria).
- Consultas ordenadas para uso direto em formulários (UI).

Detalhes técnicos
-----------------
- Conexão SQLite com:
  - `PRAGMA journal_mode=WAL;`
  - `PRAGMA busy_timeout=30000;`
  - `PRAGMA foreign_keys=ON;`
- `UNIQUE(categoria_id, nome)` garante unicidade de subcategorias dentro da categoria.
- Todas as operações aqui são simples e seguras para serem chamadas pela UI.

Dependências
------------
- sqlite3
- pandas
- typing (Optional, List, Tuple)

"""

import sqlite3
import pandas as pd
from typing import Optional, List, Tuple


class CategoriasRepository:
    """
    Repositório de categorias e subcategorias.

    Parâmetros:
        db_path (str): Caminho do arquivo SQLite.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_schema()

    def _get_conn(self) -> sqlite3.Connection:
        """
        Abre conexão SQLite configurada com PRAGMAs de confiabilidade/performance.
        """
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _ensure_schema(self) -> None:
        """
        Cria o schema mínimo, caso não exista (idempotente).
        Tabelas:
            - categorias_saida(id, nome UNIQUE)
            - subcategorias_saida(id, categoria_id → categorias_saida.id, nome, UNIQUE(categoria_id, nome))
        """
        with self._get_conn() as conn:
            conn.executescript(
                """
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
                """
            )
            conn.commit()

    # ------------- categorias -------------

    def listar_categorias(self) -> pd.DataFrame:
        """
        Retorna um DataFrame com as categorias cadastradas.

        Colunas:
            - id (int)
            - nome (str)
        Ordenação:
            - por nome (ASC)
        """
        with self._get_conn() as conn:
            return pd.read_sql("SELECT id, nome FROM categorias_saida ORDER BY nome", conn)

    def adicionar_categoria(self, nome: str) -> Optional[int]:
        """
        Cria (se não existir) uma categoria e retorna seu `id`.

        Parâmetros:
            nome (str): Nome da categoria (trim aplicado).

        Retorno:
            Optional[int]: id da categoria; `None` se nome inválido.
        """
        nome = (nome or "").strip()
        if not nome:
            return None
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO categorias_saida (nome) VALUES (?)", (nome,))
            conn.commit()
            row = conn.execute("SELECT id FROM categorias_saida WHERE nome = ? LIMIT 1", (nome,)).fetchone()
            return row[0] if row else None

    def excluir_categoria(self, categoria_id: int) -> None:
        """
        Exclui a categoria pelo `categoria_id`.

        Observação:
            - Subcategorias associadas são removidas por `ON DELETE CASCADE`.
        """
        with self._get_conn() as conn:
            conn.execute("DELETE FROM categorias_saida WHERE id = ?", (categoria_id,))
            conn.commit()

    def obter_categoria_por_nome(self, nome: str) -> Optional[Tuple[int, str]]:
        """
        Retorna `(id, nome)` da categoria pelo nome exato.

        Parâmetros:
            nome (str): Nome da categoria.

        Retorno:
            Optional[Tuple[int, str]]: Par (id, nome), ou `None` se não existir.
        """
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT id, nome FROM categorias_saida WHERE nome = ? LIMIT 1",
                (nome,),
            ).fetchone()
            return (row[0], row[1]) if row else None

    # ------------- subcategorias -------------

    def listar_subcategorias(self, categoria_id: int) -> pd.DataFrame:
        """
        Retorna um DataFrame com as subcategorias da `categoria_id`.

        Colunas:
            - id (int)
            - nome (str)
        Ordenação:
            - por nome (ASC)
        """
        with self._get_conn() as conn:
            return pd.read_sql(
                "SELECT id, nome FROM subcategorias_saida WHERE categoria_id = ? ORDER BY nome",
                conn,
                params=(categoria_id,),
            )

    def adicionar_subcategoria(self, categoria_id: int, nome: str) -> Optional[int]:
        """
        Cria (se não existir) uma subcategoria para a `categoria_id` e retorna seu `id`.

        Parâmetros:
            categoria_id (int): Chave da categoria.
            nome (str): Nome da subcategoria (trim aplicado).

        Retorno:
            Optional[int]: id da subcategoria; `None` se nome inválido.
        """
        nome = (nome or "").strip()
        if not nome:
            return None
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT OR IGNORE INTO subcategorias_saida (categoria_id, nome)
                VALUES (?, ?)
                """,
                (int(categoria_id), nome),
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT id FROM subcategorias_saida
                 WHERE categoria_id = ? AND nome = ? LIMIT 1
                """,
                (int(categoria_id), nome),
            ).fetchone()
            return row[0] if row else None

    def excluir_subcategoria(self, subcat_id: int) -> None:
        """
        Exclui a subcategoria pelo `subcat_id`.
        """
        with self._get_conn() as conn:
            conn.execute("DELETE FROM subcategorias_saida WHERE id = ?", (subcat_id,))
            conn.commit()

    def obter_sub_por_nome(self, categoria_id: int, nome: str) -> Optional[Tuple[int, str]]:
        """
        Retorna `(id, nome)` da subcategoria pelo nome exato dentro da `categoria_id`.

        Parâmetros:
            categoria_id (int): Chave da categoria.
            nome (str): Nome da subcategoria.

        Retorno:
            Optional[Tuple[int, str]]: Par (id, nome), ou `None` se não existir.
        """
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT id, nome FROM subcategorias_saida
                 WHERE categoria_id = ? AND nome = ? LIMIT 1
                """,
                (int(categoria_id), nome),
            ).fetchone()
            return (row[0], row[1]) if row else None


# (Opcional) API pública explícita
__all__ = [
    "CategoriasRepository",
]
