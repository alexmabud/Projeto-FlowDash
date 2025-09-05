"""
Infraestrutura do Ledger.

Utilitários comuns para serviços do Ledger:
- Garantir linhas em `saldos_caixas` e `saldos_bancos`.
- Criar/ajustar colunas dinâmicas de bancos de forma segura.
- Helpers de data (somar meses preservando fim de mês; competência de cartão).
- Helper para padronizar a coluna `observacao` (saídas).
- Helper para registrar linhas padronizadas em `movimentacoes_bancarias` (com idempotência por trans_uid).

Responsabilidades:
- Manter a base de saldos por dia pronta para operações.
- Atualizar saldos de bancos/caixas com segurança (sem SQL injection).
- Calcular a competência de compras em cartão (fechamento/vencimento).
- Montar a observação de saídas no padrão solicitado.

Dependências:
- sqlite3 (conexão fornecida pelo chamador).

Notas de segurança:
- Nunca interpolar entrada do usuário diretamente em SQL.
- Para colunas dinâmicas de bancos, validar o nome com whitelist e sempre quotar.
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import calendar
import hashlib
import logging
import os
import re
import sqlite3
import sys
import unicodedata
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Optional

# Garante que a raiz do projeto (<raiz>/services/ledger/..) esteja no sys.path
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

logger = logging.getLogger(__name__)

__all__ = [
    "_InfraLedgerMixin",
    "log_mov_bancaria",
    "_fmt_obs_saida",
    "gerar_trans_uid",
    "vincular_mov_a_parcela_boleto",
]


class _InfraLedgerMixin:
    """Mixin com utilitários de infraestrutura para o Ledger."""

    # ------------------------------------------------------------------
    # saldos_caixas / saldos_bancos
    # ------------------------------------------------------------------
    def _garantir_linha_saldos_caixas(self, conn: sqlite3.Connection, data: str) -> None:
        """Garante a existência da linha em `saldos_caixas` para a data."""
        cur = conn.execute("SELECT 1 FROM saldos_caixas WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute(
                """
                INSERT INTO saldos_caixas (data, caixa, caixa_2, caixa_vendas, caixa2_dia, caixa_total, caixa2_total)
                VALUES (?, 0, 0, 0, 0, 0, 0)
                """,
                (data,),
            )
            logger.debug("Criada linha em saldos_caixas para data=%s", data)

    def _garantir_linha_saldos_bancos(self, conn: sqlite3.Connection, data: str) -> None:
        """Garante a existência da linha em `saldos_bancos` para a data."""
        cur = conn.execute("SELECT 1 FROM saldos_bancos WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("INSERT OR IGNORE INTO saldos_bancos (data) VALUES (?)", (data,))
            logger.debug("Criada linha em saldos_bancos para data=%s", data)

    # --------- validação segura de nome de coluna (bancos dinâmicos) --------
    _COL_RE = re.compile(r"^[A-Za-z0-9_ ]{1,64}$")  # letras, números, underscore e espaço

    def _validar_nome_coluna_banco(self, banco_col: str) -> str:
        """Valida o nome da coluna de banco (whitelist) e retorna a versão segura."""
        banco_col = (banco_col or "").strip()
        if not self._COL_RE.match(banco_col):
            raise ValueError(f"Nome de coluna de banco inválido: {banco_col!r}")
        return banco_col

    def _ajustar_banco_dynamic(
        self,
        conn: sqlite3.Connection,
        banco_col: str,
        delta: float,
        data: str,
    ) -> None:
        """Ajusta dinamicamente a coluna do banco em `saldos_bancos`.

        - Cria a coluna do banco se não existir (DEFAULT 0.0).
        - Garante a linha do dia.
        - Aplica o `delta` na coluna.
        """
        banco_col = self._validar_nome_coluna_banco(banco_col)

        # garante existência da coluna
        cols = [r[1] for r in conn.execute("PRAGMA table_info(saldos_bancos)").fetchall()]
        if banco_col not in cols:
            conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_col}" REAL DEFAULT 0.0;')
            logger.debug("Criada coluna dinâmica em saldos_bancos: %s", banco_col)

        # garante a linha do dia
        self._garantir_linha_saldos_bancos(conn, data)

        # aplica o delta
        conn.execute(
            f'UPDATE saldos_bancos SET "{banco_col}" = COALESCE("{banco_col}",0) + ? WHERE data = ?',
            (float(delta), data),
        )
        logger.debug("Ajustado banco_col=%s em %s com delta=%.2f", banco_col, data, float(delta))

    # ------------------------------------------------------------------
    # data utils
    # ------------------------------------------------------------------
    @staticmethod
    def _add_months(dt: date, months: int) -> date:
        """Soma meses preservando fim de mês quando aplicável."""
        y = dt.year + (dt.month - 1 + months) // 12
        m = (dt.month - 1 + months) % 12 + 1
        d = min(
            dt.day,
            [
                31,
                29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28,
                31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
            ][m - 1],
        )
        return date(y, m, d)

    def _competencia_compra(self, compra_dt: datetime, vencimento_dia: int, dias_fechamento: int) -> str:
        """Calcula a competência contábil da compra (regra de cartão)."""
        y, m = compra_dt.year, compra_dt.month
        last = calendar.monthrange(y, m)[1]
        venc_d = min(int(vencimento_dia), last)

        venc_date = datetime(y, m, venc_d)
        fechamento_date = venc_date - timedelta(days=int(dias_fechamento))

        if compra_dt > fechamento_date:
            if m == 12:
                y += 1
                m = 1
            else:
                m += 1

        comp = f"{y:04d}-{m:02d}"
        logger.debug(
            "competencia_compra: compra_dt=%s venci_dia=%s fechamento=%s => %s",
            compra_dt, vencimento_dia, fechamento_date.date(), comp
        )
        return comp


# -----------------------------------------------------------------------------
# Helpers para observações e movimentações bancárias
# -----------------------------------------------------------------------------
def _sem_acentos(s: str) -> str:
    """Remove acentos (normalização simples p/ 'DEBITO' e 'CREDITO')."""
    if not s:
        return s
    return "".join(
        ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn"
    )


def _fmt_obs_saida(
    *,
    forma: str,
    valor: float,
    categoria: str | None,
    subcategoria: str | None,
    descricao: str | None,
    banco: str | None = None,
    cartao: str | None = None,
    parcelas: int | None = None,
) -> str:
    """Padroniza a `observacao` para `movimentacoes_bancarias` em SAÍDAS."""
    f_raw = (forma or "").strip().upper()
    f = _sem_acentos(f_raw)
    if f == "DEBITO A VISTA":
        f = "DEBITO"
    elif f in {"CARTAO", "CARTAO DE CREDITO"}:
        f = "CREDITO"

    validos = {"DINHEIRO", "PIX", "DEBITO", "CREDITO", "BOLETO"}
    if f not in validos:
        f = _sem_acentos(f_raw) or "SAIDA"

    if f == "DEBITO":
        head = f"Lançamento SAÍDA DEBITO {(banco or '').strip()}".rstrip()
    elif f == "CREDITO":
        head = f"Lançamento SAÍDA CREDITO {(cartao or '').strip()}".rstrip()
    elif f == "DINHEIRO":
        head = "Lançamento SAÍDA DINHEIRO"
    elif f == "PIX":
        head = "Lançamento SAÍDA PIX"
    elif f == "BOLETO":
        head = "Lançamento SAÍDA BOLETO"
    else:
        head = f"Lançamento SAÍDA {f}"

    try:
        vtxt = f"R${float(valor):.2f}"
    except Exception:
        vtxt = "R$0.00"

    cat = (categoria or "").strip() or "-"
    sub = (subcategoria or "").strip() or "-"
    desc = (descricao or "").strip()

    trilha = f" • {cat}, {sub}"
    if desc:
        trilha += f", • {desc}"

    if f == "CREDITO" and isinstance(parcelas, int) and parcelas >= 2:
        trilha += f" • {parcelas}x"

    return f"{head} {vtxt}{trilha}"


def _resolve_usuario(u: Any) -> str:
    """Normaliza a informação de usuário em string segura."""
    try:
        if isinstance(u, dict):
            for k in ("nome", "name", "username", "user", "email", "login"):
                v = u.get(k)
                if v:
                    return str(v).strip()
            return "sistema"
        s = str(u).strip() if u is not None else ""
        return s or "sistema"
    except Exception:
        return "sistema"


def _ensure_mov_cols(cur: sqlite3.Cursor) -> None:
    """Garante colunas em `movimentacoes_bancarias` (idempotente): usuario, data_hora, trans_uid."""
    cols = {row[1] for row in cur.execute("PRAGMA table_info(movimentacoes_bancarias);").fetchall()}
    if "usuario" not in cols:
        cur.execute("ALTER TABLE movimentacoes_bancarias ADD COLUMN usuario TEXT;")
    if "data_hora" not in cols:
        cur.execute("ALTER TABLE movimentacoes_bancarias ADD COLUMN data_hora TEXT;")
    if "trans_uid" not in cols:
        # sem UNIQUE aqui para compatibilidade com bancos mais antigos
        cur.execute("ALTER TABLE movimentacoes_bancarias ADD COLUMN trans_uid TEXT;")


# === ID: helpers de UID/idempotência =========================================
def gerar_trans_uid(prefix: str = "mb", seed: Optional[str] = None) -> str:
    """
    Gera um UID de transação:
    - Determinístico quando `seed` é informado (SHA1-16).
    - Aleatório (UUID4) quando `seed` não é informado.
    Formato: "<prefix>:<hex>"
    """
    try:
        if seed is not None:
            h = hashlib.sha1(str(seed).encode("utf-8")).hexdigest()[:16]
            return f"{prefix}:{h}"
        return f"{prefix}:{uuid.uuid4().hex}"
    except Exception:
        # Fallback ultra defensivo
        import time
        return f"{prefix}:{int(time.time() * 1000)}"


def log_mov_bancaria(
    conn: sqlite3.Connection,
    *,
    data: str,
    banco: str,
    tipo: str,
    valor: float,
    origem: str,
    observacao: str,
    usuario: Optional[Any] = None,
    referencia_id: Optional[int] = None,
    referencia_tabela: Optional[str] = None,
    trans_uid: Optional[str] = None,
    data_hora: Optional[str] = None,
    auto_self_reference: bool = True,
) -> int:
    """
    Insere (ou reutiliza) um registro em `movimentacoes_bancarias`.

    Idempotência:
    - Se `trans_uid` for informado e já existir na tabela, retorna o `id` existente (NÃO duplica).
    - Caso contrário, gera um novo UID (via `gerar_trans_uid("mb")`) e insere normalmente.

    Observação:
    - NÃO faz commit; o chamador decide a transação.
    """
    from datetime import datetime as _dt

    cur = conn.cursor()
    _ensure_mov_cols(cur)  # garante colunas novas (usuario,data_hora,trans_uid), não quebra se já existir

    # Normalizações leves
    _tipo = (tipo or "").strip().lower() or "saida"  # padrão conservador
    if _tipo not in {"entrada", "saida", "transferencia"}:
        _tipo = "saida"

    _user = _resolve_usuario(usuario)
    _dh = data_hora or _dt.now().strftime("%Y-%m-%d %H:%M:%S")

    # Idempotência por trans_uid
    _uid = trans_uid or gerar_trans_uid("mb")
    try:
        if trans_uid:
            cur.execute(
                "SELECT id FROM movimentacoes_bancarias WHERE trans_uid = ? LIMIT 1",
                (trans_uid,),
            )
            row = cur.fetchone()
            if row and row[0]:
                # Já existe — reusa o ID
                return int(row[0])
    except Exception:
        # Se não conseguir checar (schema antigo), segue fluxo normal de INSERT
        pass

    # ORDEM CORRETA (schema base): referencia_tabela vem ANTES de referencia_id
    cur.execute(
        """
        INSERT INTO movimentacoes_bancarias
            (data, banco, tipo, valor, origem, observacao,
             referencia_tabela, referencia_id, trans_uid, usuario, data_hora)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data,
            banco,
            _tipo,
            float(valor or 0.0),
            origem,
            observacao,
            referencia_tabela,
            referencia_id,
            _uid,
            _user,
            _dh,
        ),
    )
    mov_id = int(cur.lastrowid)

    # Opcional: autorreferência quando não há vínculo externo informado
    if auto_self_reference and referencia_id is None:
        cur.execute(
            """
            UPDATE movimentacoes_bancarias
               SET referencia_id = ?, referencia_tabela = ?
             WHERE id = ?
            """,
            (mov_id, "movimentacoes_bancarias", mov_id),
        )

    return mov_id


# === Helper: vincular movimento bancário à parcela de BOLETO =================
def vincular_mov_a_parcela_boleto(caminho_banco: str, id_mov: int, parcela_id_boleto: int) -> None:
    """
    Amarra o movimento bancário (movimentacoes_bancarias.id) à parcela de BOLETO
    (contas_a_pagar_mov.id), preenchendo referencia_tabela/referencia_id.

    Uso: chame logo após criar o movimento, quando tiver `parcela_id_boleto`.
    É idempotente e silencioso se parâmetros forem inválidos.
    """
    try:
        if not id_mov or not parcela_id_boleto:
            return
        from shared.db import get_conn  # import local para evitar ciclos
        with get_conn(caminho_banco) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE movimentacoes_bancarias
                   SET referencia_tabela = 'contas_a_pagar_mov',
                       referencia_id     = ?
                 WHERE id = ?
                """,
                (int(parcela_id_boleto), int(id_mov)),
            )
            conn.commit()
    except Exception:
        # Não quebrar o fluxo principal por causa do vínculo (apenas log se tiver logger)
        try:
            import logging as _lg
            _lg.getLogger(__name__).warning(
                "Falha ao vincular mov %s à parcela boleto %s", id_mov, parcela_id_boleto
            )
        except Exception:
            pass
