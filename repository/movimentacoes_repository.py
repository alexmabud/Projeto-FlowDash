from __future__ import annotations

import sqlite3
import hashlib
from typing import Optional, Dict, Any
from utils.utils import resolve_db_path


class MovimentacoesRepository:
    """
    Repositório para operações na tabela `movimentacoes_bancarias`.

    Correções e melhorias:
    - Normalização robusta do campo `valor` (aceita float/int e strings: "R$ 1.234,56", "1.234,56", "1234.56").
    - Aceita sinônimos para valor: valor, valor_mov, valor_total, valor_parcela, valor_bruto (prioridade nesta ordem).
    - Idempotência por `trans_uid` determinístico quando não informado.
    - Migrações idempotentes: cria tabela/índices/colunas ausentes e UNIQUE por índice em `trans_uid`.
    - Tipos semânticos padronizados: "entrada" | "saida" | "transferencia" | "registro".
    """

    def __init__(self, db_path_like: Any):
        self.db_path: str = resolve_db_path(db_path_like)
        self.garantir_schema()

    # ---------------- conexões ----------------

    def _get_conn(self) -> sqlite3.Connection:
        """Abre conexão SQLite com PRAGMAs padronizados do projeto."""
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

    # ---------------- utils: valor/trans_uid ----------------

    @staticmethod
    def _pick_valor_from_aliases(payload: Dict[str, Any]) -> Any:
        """Retorna o primeiro alias de valor não-nulo encontrado."""
        for k in ("valor", "valor_mov", "valor_total", "valor_parcela", "valor_bruto"):
            if k in payload and payload[k] is not None:
                return payload[k]
        return None

    @staticmethod
    def _normalize_valor(v: Any) -> float:
        """
        Converte valor para float aceitando:
        - float/int
        - strings "R$ 1.234,56", "1.234,56", "1234.56".
        """
        if v is None:
            raise ValueError("Valor não pode ser None.")
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            raise ValueError("Valor não pode ser vazio.")
        s = s.replace("R$", "").replace("r$", "").strip()
        s = s.replace(".", "")  # remove milhar
        s = s.replace(",", ".")  # vírgula decimal -> ponto
        try:
            return float(s)
        except Exception:
            raise ValueError(f"Valor inválido: {v!r}")

    @staticmethod
    def _hash_uid(payload: Dict[str, Any]) -> str:
        """Gera um UID determinístico a partir dos campos essenciais."""
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

    # ---------------- schema / migração ----------------

    def _colunas_existentes(self, conn: sqlite3.Connection) -> set:
        rows = conn.execute("PRAGMA table_info(movimentacoes_bancarias);").fetchall()
        return {str(r["name"]) if isinstance(r, sqlite3.Row) else str(r[1]) for r in rows}

    def _garantir_unique_trans_uid(self, conn: sqlite3.Connection) -> None:
        """Garante coluna/índice UNIQUE para `trans_uid` em bases legadas."""
        existentes = self._colunas_existentes(conn)
        if "trans_uid" not in existentes:
            conn.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "trans_uid" TEXT;')
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_mov_trans_uid ON movimentacoes_bancarias(trans_uid);")

    def garantir_schema(self) -> None:
        """Cria a tabela/índices e garante colunas opcionais. Idempotente."""
        with self._get_conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS movimentacoes_bancarias (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT NOT NULL,
                    banco TEXT NOT NULL,
                    tipo TEXT NOT NULL,              -- 'entrada' | 'saida' | 'transferencia' | 'registro'
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
            existentes = self._colunas_existentes(conn)
            if "usuario" not in existentes:
                conn.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "usuario" TEXT;')
            if "data_hora" not in existentes:
                conn.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "data_hora" TEXT;')
            self._garantir_unique_trans_uid(conn)
            conn.commit()

    # ---------------- consultas / utilidades ----------------

    def ja_existe_transacao(self, trans_uid: str) -> bool:
        """True se existir uma movimentação com o `trans_uid` informado."""
        if not trans_uid:
            return False
        with self._get_conn() as conn:
            cur = conn.execute("SELECT 1 FROM movimentacoes_bancarias WHERE trans_uid = ? LIMIT 1", (trans_uid,))
            return cur.fetchone() is not None

    def obter_por_trans_uid(self, trans_uid: str) -> Optional[Dict[str, Any]]:
        """Retorna o registro de movimentação pelo trans_uid (ou None)."""
        if not trans_uid:
            return None
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM movimentacoes_bancarias WHERE trans_uid = ? LIMIT 1",
                (trans_uid,),
            ).fetchone()
            return dict(row) if row else None

    # ---------------- inserts brutos ----------------

    def inserir_log(
        self,
        data: str,
        banco: str,
        tipo: str,
        valor: Any,
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
        ⚠️ Normaliza `valor` e BLOQUEIA salvar 'entrada'/'saida' com valor 0.
        """
        valor_norm = self._normalize_valor(valor)

        # 🚫 trava de segurança: não permitir salvar 0 para entrada/saida
        if tipo in ("entrada", "saida") and float(valor_norm) == 0.0:
            raise ValueError(
                "Valor da movimentação não pode ser zero para 'entrada'/'saida'. "
                "Verifique o ponto de chamada (o valor pode estar vindo vazio ou no alias errado)."
            )

        with self._get_conn() as conn:
            # Migração defensiva em runtime
            existentes = self._colunas_existentes(conn)
            if "usuario" not in existentes:
                conn.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "usuario" TEXT;')
            if "data_hora" not in existentes:
                conn.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "data_hora" TEXT;')
            self._garantir_unique_trans_uid(conn)

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
                    float(valor_norm),
                    origem or "",
                    observacao,
                    referencia_tabela,
                    referencia_id,
                    trans_uid,
                    usuario,
                    data_hora,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    # ---------------- helpers idempotentes / semânticos ----------------

    def _resolver_payload_e_uid(
        self,
        *,
        data: str,
        banco: str,
        tipo: str,
        origem: str,
        observacao: Optional[str],
        referencia_tabela: Optional[str],
        referencia_id: Optional[int],
        trans_uid: Optional[str],
        valor: Any = None,
        **extras: Any,
    ) -> tuple[Dict[str, Any], str]:
        """
        Monta payload com `valor` normalizado (aceitando aliases) e define `trans_uid`.
        Lança erro se nenhum valor for encontrado em `valor` nem nos aliases.
        """
        bruto = valor if valor is not None else self._pick_valor_from_aliases(extras)
        if bruto is None:
            raise ValueError(
                "Nenhum valor informado para a movimentação. "
                "Use `valor` ou um dos aliases: valor_mov, valor_total, valor_parcela, valor_bruto."
            )
        valor_norm = self._normalize_valor(bruto)

        payload = {
            "data": data,
            "banco": banco or "",
            "tipo": tipo,
            "valor": float(valor_norm),
            "origem": origem or "",
            "observacao": observacao,
            "referencia_tabela": referencia_tabela,
            "referencia_id": referencia_id,
        }
        uid = trans_uid or self._hash_uid(payload)
        return payload, uid

    def inserir_generico(
        self,
        *,
        data: str,
        banco: str,
        tipo: str,  # "entrada" | "saida" | "transferencia" | "registro"
        valor: Any = None,
        origem: str,
        observacao: Optional[str] = None,
        referencia_tabela: Optional[str] = None,
        referencia_id: Optional[int] = None,
        trans_uid: Optional[str] = None,
        usuario: Optional[str] = None,
        data_hora: Optional[str] = None,
        **extras: Any,
    ) -> int:
        """
        Insere uma movimentação com **idempotência** e normalização de `valor`.

        - Aceita aliases para `valor` via **extras.
        - Se `trans_uid` não for fornecido, gera hash determinístico do payload.
        - Se já existir `trans_uid`, retorna o ID existente (ou -1 se não localizar).
        """
        payload, uid = self._resolver_payload_e_uid(
            data=data,
            banco=banco,
            tipo=tipo,
            origem=origem,
            observacao=observacao,
            referencia_tabela=referencia_tabela,
            referencia_id=referencia_id,
            trans_uid=trans_uid,
            valor=valor,
            **extras,
        )

        if payload["valor"] == 0.0 and tipo in ("entrada", "saida"):
            raise ValueError("Valor não pode ser zero para 'entrada'/'saida'.")

        if self.ja_existe_transacao(uid):
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT id FROM movimentacoes_bancarias WHERE trans_uid = ? LIMIT 1",
                    (uid,),
                ).fetchone()
                return int(row[0]) if row else -1

        return self.inserir_log(
            data=payload["data"],
            banco=payload["banco"],
            tipo=payload["tipo"],
            valor=payload["valor"],
            origem=payload["origem"],
            observacao=payload["observacao"],
            referencia_tabela=payload["referencia_tabela"],
            referencia_id=payload["referencia_id"],
            trans_uid=uid,
            usuario=usuario,
            data_hora=data_hora,
        )

    def registrar_entrada(
        self,
        *,
        data: str,
        banco: str,
        valor: Any = None,
        origem: str = "",
        observacao: Optional[str] = None,
        referencia_tabela: Optional[str] = None,
        referencia_id: Optional[int] = None,
        trans_uid: Optional[str] = None,
        usuario: Optional[str] = None,
        data_hora: Optional[str] = None,
        **extras: Any,
    ) -> int:
        """Atalho semântico para registrar **entrada** (valor > 0)."""
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
            **extras,
        )

    def registrar_saida(
        self,
        *,
        data: str,
        banco: str,
        valor: Any = None,
        origem: str = "",
        observacao: Optional[str] = None,
        referencia_tabela: Optional[str] = None,
        referencia_id: Optional[int] = None,
        trans_uid: Optional[str] = None,
        usuario: Optional[str] = None,
        data_hora: Optional[str] = None,
        **extras: Any,
    ) -> int:
        """Atalho semântico para registrar **saída** (valor > 0)."""
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
            **extras,
        )

    # ---------------- migração opcional pública ----------------

    def criar_indice_unique_trans_uid(self) -> None:
        """Força (re)criação do índice UNIQUE de `trans_uid`, se necessário."""
        with self._get_conn() as conn:
            self._garantir_unique_trans_uid(conn)
            conn.commit()


__all__ = ["MovimentacoesRepository"]
