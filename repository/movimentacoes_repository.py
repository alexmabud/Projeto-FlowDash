import sqlite3
import hashlib
from typing import Optional, Dict, Any

class MovimentacoesRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.garantir_schema()  # garante tabela/índices na inicialização

    def _get_conn(self):
        # Conexão padrão do projeto (WAL + busy timeout + FKs)
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def garantir_schema(self):
        with self._get_conn() as conn:
            conn.executescript("""
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
            CREATE INDEX IF NOT EXISTS idx_mov_data ON movimentacoes_bancarias(data);
            CREATE INDEX IF NOT EXISTS idx_mov_banco ON movimentacoes_bancarias(banco);
            """)
            conn.commit()

    # ---------- existentes ----------
    def ja_existe_transacao(self, trans_uid: str) -> bool:
        if not trans_uid:
            return False
        with self._get_conn() as conn:
            cur = conn.execute(
                "SELECT 1 FROM movimentacoes_bancarias WHERE trans_uid = ? LIMIT 1",
                (trans_uid,)
            )
            return cur.fetchone() is not None

    def inserir_log(self, data: str, banco: str, tipo: str, valor: float,
                    origem: str, observacao: str, referencia_tabela: Optional[str],
                    referencia_id: Optional[int], trans_uid: Optional[str]) -> int:
        # mantém retrocompatibilidade com chamadas antigas
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao,
                     referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (data, banco or "", tipo, float(valor), origem, observacao,
                  referencia_tabela, referencia_id, trans_uid))
            conn.commit()
            return cur.lastrowid

    # ---------- novos helpers (idempotência e semânticos) ----------
    def _hash_uid(self, payload: Dict[str, Any]) -> str:
        """
        Gera um UID determinístico a partir dos campos essenciais.
        Usado quando o caller não fornece trans_uid explícito.
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
        tipo: str,               # "entrada" | "saida" | "transferencia" (se usar)
        valor: float,
        origem: str,
        observacao: Optional[str] = None,
        referencia_tabela: Optional[str] = None,
        referencia_id: Optional[int] = None,
        trans_uid: Optional[str] = None
    ) -> int:
        """
        Insere em movimentacoes_bancarias com idempotência.
        - Se trans_uid não vier, será gerado de forma determinística.
        - Retorna o id inserido; se já existir (mesmo trans_uid), não duplica.
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
            "referencia_id": referencia_id
        }
        uid = trans_uid or self._hash_uid(payload)

        # idempotência via trans_uid
        if self.ja_existe_transacao(uid):
            # retorna o id existente, se quiser buscar:
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT id FROM movimentacoes_bancarias WHERE trans_uid=? LIMIT 1",
                    (uid,)
                ).fetchone()
                return int(row[0]) if row else -1

        # insere
        return self.inserir_log(
            data=data,
            banco=banco or "",
            tipo=tipo,
            valor=float(valor),
            origem=origem,
            observacao=observacao or None,
            referencia_tabela=referencia_tabela,
            referencia_id=referencia_id,
            trans_uid=uid
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
        trans_uid: Optional[str] = None
    ) -> int:
        if valor <= 0:
            raise ValueError("Entrada deve ter valor > 0.")
        return self.inserir_generico(
            data=data, banco=banco, tipo="entrada", valor=valor,
            origem=origem, observacao=observacao,
            referencia_tabela=referencia_tabela, referencia_id=referencia_id,
            trans_uid=trans_uid
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
        trans_uid: Optional[str] = None
    ) -> int:
        if valor <= 0:
            raise ValueError("Saída deve ter valor > 0.")
        return self.inserir_generico(
            data=data, banco=banco, tipo="saida", valor=valor,
            origem=origem, observacao=observacao,
            referencia_tabela=referencia_tabela, referencia_id=referencia_id,
            trans_uid=trans_uid
        )