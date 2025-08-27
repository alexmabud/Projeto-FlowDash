from __future__ import annotations

import sqlite3
import hashlib
from typing import Optional, Dict, Any
from utils.utils import resolve_db_path


class MovimentacoesRepository:
    """
    Repositório para operações na tabela `movimentacoes_bancarias`.

    Parâmetros:
        db_path_like (Any): Caminho do arquivo SQLite (.db) OU objeto com
            atributo `db_path` / `caminho_banco` / `database`.
    """
    def __init__(self, db_path_like: Any):
        # Aceita string/Path/objeto com atributo de caminho
        self.db_path: str = resolve_db_path(db_path_like)
        self.garantir_schema()  # garante tabela/índices/colunas na inicialização

    def _get_conn(self) -> sqlite3.Connection:
        """
        Abre conexão SQLite com PRAGMAs padronizados do projeto
        (WAL, busy timeout, foreign keys, synchronous NORMAL)
        e parsing de tipos (DATE/DATETIME).
        """
        conn = sqlite3.connect(
            self.db_path,
            timeout=30,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.row_factory = sqlite3.Row
        return conn

    def _colunas_existentes(self, conn: sqlite3.Connection) -> set:
        rows = conn.execute("PRAGMA table_info(movimentacoes_bancarias);").fetchall()
        return {str(r["name"]) if isinstance(r, sqlite3.Row) else str(r[1]) for r in rows}

    def garantir_schema(self) -> None:
        """
        Cria a tabela `movimentacoes_bancarias` e índices, se não existirem.
        Também garante colunas opcionais novas (usuario, data_hora).
        Idempotente.
        """
        with self._get_conn() as conn:
            # Tabela base
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

            # Migração leve: garantir colunas novas
            existentes = self._colunas_existentes(conn)
            if "usuario" not in existentes:
                conn.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "usuario" TEXT;')
            if "data_hora" not in existentes:
                conn.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "data_hora" TEXT;')

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
        *,
        usuario: Optional[str] = None,
        data_hora: Optional[str] = None,
    ) -> int:
        """
        Insere uma linha bruta em `movimentacoes_bancarias`.
        Mantém retrocompatibilidade com chamadas antigas.
        """
        with self._get_conn() as conn:
            # Garantir que as colunas novas existem (execuções antigas em runtime)
            existentes = self._colunas_existentes(conn)
            if "usuario" not in existentes:
                conn.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "usuario" TEXT;')
            if "data_hora" not in existentes:
                conn.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "data_hora" TEXT;')

            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao,
                     referencia_tabela, referencia_id, trans_uid,
                     usuario, data_hora)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    usuario,
                    data_hora,
                ),
            )
            conn.commit()
            return cur.lastrowid

    # ---------- novos helpers (idempotência e semânticos) ----------

    def _hash_uid(self, payload: Dict[str, Any]) -> str:
        """
        Gera um UID determinístico a partir dos campos essenciais.
        Usado quando o caller não fornece `trans_uid` explicitamente.

        (Não inclui usuario/data_hora no hash para não quebrar idempotência.)
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
        usuario: Optional[str] = None,
        data_hora: Optional[str] = None,
    ) -> int:
        """
        Insere uma movimentação com **idempotência**.

        Regras:
            - Se `trans_uid` não for fornecido, é gerado de forma determinística
              a partir do payload (hash SHA-256).
            - Se já houver uma linha com o mesmo `trans_uid`, não duplica e
              retorna o `id` existente (ou `-1` se não localizar, por segurança).
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
            usuario=usuario,
            data_hora=data_hora,
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
        usuario: Optional[str] = None,
        data_hora: Optional[str] = None,
    ) -> int:
        """
        Atalho semântico para registrar **entrada** (valor > 0).
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
            usuario=usuario,
            data_hora=data_hora,
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
        usuario: Optional[str] = None,
        data_hora: Optional[str] = None,
    ) -> int:
        """
        Atalho semântico para registrar **saída** (valor > 0).
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
            usuario=usuario,
            data_hora=data_hora,
        )


__all__ = ["MovimentacoesRepository"]
