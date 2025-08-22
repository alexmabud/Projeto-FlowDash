"""
Módulo Movimentações (Repositório)
==================================

Este módulo define a classe `MovimentacoesRepository`, responsável por gerenciar
a tabela **`movimentacoes_bancarias`** no SQLite. Centraliza criação de schema,
registro idempotente de entradas/saídas e consultas utilitárias.

Funcionalidades principais
--------------------------
- Criação automática do schema e índices (idempotente).
- Registro de **entradas** e **saídas** com metadados (origem, observação).
- Suporte a **idempotência** via `trans_uid` (explícito ou determinístico).
- Verificação de existência/duplicidade por `trans_uid`.

Detalhes técnicos
-----------------
- Conexão SQLite com:
  - `PRAGMA journal_mode=WAL;`
  - `PRAGMA busy_timeout=30000;`
  - `PRAGMA foreign_keys=ON;`
- Índices por data e banco.
- `trans_uid` com restrição UNIQUE para evitar duplicações.

Dependências
------------
- sqlite3
- hashlib
- typing (Optional, Dict, Any)
"""

import sqlite3
import hashlib
from typing import Optional, Dict, Any


class MovimentacoesRepository:
    """
    Repositório para operações na tabela `movimentacoes_bancarias`.

    Parâmetros:
        db_path (str): Caminho do arquivo SQLite.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.garantir_schema()  # garante tabela/índices na inicialização

    def _get_conn(self) -> sqlite3.Connection:
        """
        Abre conexão SQLite com PRAGMAs padronizados do projeto
        (WAL, busy timeout, foreign keys).
        """
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def garantir_schema(self) -> None:
        """
        Cria a tabela `movimentacoes_bancarias` e índices, se não existirem.
        Idempotente: pode ser chamado várias vezes sem efeitos colaterais.
        """
        with self._get_conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS movimentacoes_bancarias (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT NOT NULL,
                    banco TEXT NOT NULL,
                    tipo TEXT NOT NULL,              -- 'saida', 'entrada', etc.
                    valor REAL NOT NULL,
                    origem TEXT NOT NULL,
                    observacao TEXT,
                    referencia_tabela TEXT,
                    referencia_id INTEGER,
                    trans_uid TEXT UNIQUE
                );
                CREATE INDEX IF NOT EXISTS idx_mov_data  ON movimentacoes_bancarias(data);
                CREATE INDEX IF NOT EXISTS idx_mov_banco ON movimentacoes_bancarias(banco);
                """
            )
            conn.commit()

    # ---------- existentes ----------

    def ja_existe_transacao(self, trans_uid: str) -> bool:
        """
        Verifica se já existe movimentação com o `trans_uid` informado.

        Parâmetros:
            trans_uid (str): Identificador único da transação.

        Retorno:
            bool: True se existir, False caso contrário.
        """
        if not trans_uid:
            return False
        with self._get_conn() as conn:
            cur = conn.execute(
                "SELECT 1 FROM movimentacoes_bancarias WHERE trans_uid = ? LIMIT 1",
                (trans_uid,),
            )
            return cur.fetchone() is not None

    def inserir_log(
        self,
        data: str,
        banco: str,
        tipo: str,
        valor: float,
        origem: str,
        observacao: Optional[str],
        referencia_tabela: Optional[str],
        referencia_id: Optional[int],
        trans_uid: Optional[str],
    ) -> int:
        """
        Insere uma linha bruta em `movimentacoes_bancarias`.
        Mantém retrocompatibilidade com chamadas antigas.

        Retorno:
            int: `id` da linha inserida.
        """
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao,
                     referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data,
                    banco or "",
                    tipo,
                    float(valor),
                    origem,
                    observacao,
                    referencia_tabela,
                    referencia_id,
                    trans_uid,
                ),
            )
            conn.commit()
            return cur.lastrowid

    # ---------- novos helpers (idempotência e semânticos) ----------

    def _hash_uid(self, payload: Dict[str, Any]) -> str:
        """
        Gera um UID determinístico a partir dos campos essenciais.
        Usado quando o caller não fornece `trans_uid` explicitamente.
        """
        base = (
            str(payload.get("data", "")),
            str(payload.get("banco", "")),
            str(payload.get("tipo", "")),
            f"{float(payload.get('valor', 0.0)):.6f}",
            str(payload.get("origem", "")),
            str(payload.get("referencia_tabela", "")),
            str(payload.get("referencia_id", "")),
            str(payload.get("observacao", "")),
        )
        return hashlib.sha256("|".join(base).encode("utf-8")).hexdigest()

    def inserir_generico(
        self,
        *,
        data: str,
        banco: str,
        tipo: str,  # "entrada" | "saida" | "transferencia" (se usar)
        valor: float,
        origem: str,
        observacao: Optional[str] = None,
        referencia_tabela: Optional[str] = None,
        referencia_id: Optional[int] = None,
        trans_uid: Optional[str] = None,
    ) -> int:
        """
        Insere uma movimentação com **idempotência**.

        Regras:
            - Se `trans_uid` não for fornecido, é gerado de forma determinística
              a partir do payload (hash SHA-256).
            - Se já houver uma linha com o mesmo `trans_uid`, não duplica e
              retorna o `id` existente (ou `-1` se não localizar, por segurança).

        Retorno:
            int: `id` da movimentação (nova ou existente).
        """
        if not valor or float(valor) == 0.0:
            raise ValueError("Valor não pode ser zero.")

        payload = {
            "data": data,
            "banco": banco or "",
            "tipo": tipo,
            "valor": float(valor),
            "origem": origem,
            "observacao": observacao,
            "referencia_tabela": referencia_tabela,
            "referencia_id": referencia_id,
        }
        uid = trans_uid or self._hash_uid(payload)

        # idempotência via trans_uid
        if self.ja_existe_transacao(uid):
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT id FROM movimentacoes_bancarias WHERE trans_uid=? LIMIT 1",
                    (uid,),
                ).fetchone()
                return int(row[0]) if row else -1

        # insere (novo)
        return self.inserir_log(
            data=data,
            banco=banco or "",
            tipo=tipo,
            valor=float(valor),
            origem=origem,
            observacao=observacao or None,
            referencia_tabela=referencia_tabela,
            referencia_id=referencia_id,
            trans_uid=uid,
        )

    def registrar_entrada(
        self,
        *,
        data: str,
        banco: str,
        valor: float,
        origem: str,
        observacao: Optional[str] = None,
        referencia_tabela: Optional[str] = None,
        referencia_id: Optional[int] = None,
        trans_uid: Optional[str] = None,
    ) -> int:
        """
        Atalho semântico para registrar **entrada** (valor > 0).

        Retorno:
            int: `id` da movimentação (nova ou existente).
        """
        if valor <= 0:
            raise ValueError("Entrada deve ter valor > 0.")
        return self.inserir_generico(
            data=data,
            banco=banco,
            tipo="entrada",
            valor=valor,
            origem=origem,
            observacao=observacao,
            referencia_tabela=referencia_tabela,
            referencia_id=referencia_id,
            trans_uid=trans_uid,
        )

    def registrar_saida(
        self,
        *,
        data: str,
        banco: str,
        valor: float,
        origem: str,
        observacao: Optional[str] = None,
        referencia_tabela: Optional[str] = None,
        referencia_id: Optional[int] = None,
        trans_uid: Optional[str] = None,
    ) -> int:
        """
        Atalho semântico para registrar **saída** (valor > 0).

        Retorno:
            int: `id` da movimentação (nova ou existente).
        """
        if valor <= 0:
            raise ValueError("Saída deve ter valor > 0.")
        return self.inserir_generico(
            data=data,
            banco=banco,
            tipo="saida",
            valor=valor,
            origem=origem,
            observacao=observacao,
            referencia_tabela=referencia_tabela,
            referencia_id=referencia_id,
            trans_uid=trans_uid,
        )


# (Opcional) API pública explícita
__all__ = ["MovimentacoesRepository"]
