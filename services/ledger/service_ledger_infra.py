"""
service_ledger_infra.py — Infraestrutura do Ledger.

Resumo:
    Utilitários comuns para serviços do Ledger:
    - Garantir linhas em `saldos_caixas` e `saldos_bancos`.
    - Criar/ajustar colunas dinâmicas de bancos de forma segura.
    - Helpers de data (somar meses preservando fim de mês; competência de cartão).

Responsabilidades:
    - Manter a base de saldos por dia pronta para operações.
    - Atualizar saldos de bancos/caixas com segurança (sem SQL injection).
    - Calcular a competência de compras em cartão (fechamento/vencimento).

Depende de:
    - sqlite3 (conexão é fornecida pelo chamador)

Notas de segurança:
    - Nunca interpolar entrada do usuário diretamente em SQL.
    - Para colunas dinâmicas de bancos, validar o nome com whitelist e *sempre* quotar.
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Imports + bootstrap de caminho (robusto ao executar via Streamlit)
# -----------------------------------------------------------------------------

import calendar
import logging
import re
import sqlite3
from datetime import date, datetime, timedelta
from typing import Optional, Any
import uuid
import os
import sys

# Garante que a raiz do projeto (<raiz>/services/ledger/..) esteja no sys.path
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

logger = logging.getLogger(__name__)

__all__ = ["_InfraLedgerMixin", "log_mov_bancaria"]



class _InfraLedgerMixin:
    """Mixin com utilitários de infraestrutura para o Ledger."""

    # ----------------------------------------------------------------------
    # saldos_caixas / saldos_bancos
    # ----------------------------------------------------------------------
    def _garantir_linha_saldos_caixas(self, conn: sqlite3.Connection, data: str) -> None:
        """Garante a existência da linha em `saldos_caixas` para a data.

        Args:
            conn: Conexão SQLite aberta.
            data: Data no formato `YYYY-MM-DD`.
        """
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
        """Garante a existência da linha em `saldos_bancos` para a data.

        Args:
            conn: Conexão SQLite aberta.
            data: Data no formato `YYYY-MM-DD`.
        """
        cur = conn.execute("SELECT 1 FROM saldos_bancos WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("INSERT OR IGNORE INTO saldos_bancos (data) VALUES (?)", (data,))
            logger.debug("Criada linha em saldos_bancos para data=%s", data)

    # --------- validação segura de nome de coluna (bancos dinâmicos) --------------------
    _COL_RE = re.compile(r"^[A-Za-z0-9_ ]{1,64}$")  # letras, números, underscore e espaço

    def _validar_nome_coluna_banco(self, banco_col: str) -> str:
        """Valida o nome da coluna de banco (whitelist) e retorna a versão segura.

        Regras:
            - 1 a 64 caracteres: A–Z, a–z, 0–9, `_` e espaço.
            - Sem aspas, ponto-e-vírgula, quebras de linha, etc.

        Args:
            banco_col: Nome solicitado para a coluna.

        Returns:
            str: Nome aprovado (mesmo texto, se válido).

        Raises:
            ValueError: Se o nome não obedecer à whitelist.
        """
        banco_col = (banco_col or "").strip()
        if not self._COL_RE.match(banco_col):
            raise ValueError(f"Nome de coluna de banco inválido: {banco_col!r}")
        return banco_col

    def _ajustar_banco_dynamic(self, conn: sqlite3.Connection, banco_col: str, delta: float, data: str) -> None:
        """Ajusta dinamicamente a coluna do banco em `saldos_bancos`.

        Cria a coluna do banco (se necessário) e aplica o delta na data indicada.

        Args:
            conn: Conexão SQLite aberta.
            banco_col: Nome exato da coluna do banco (validado por whitelist).
            delta: Variação a aplicar (pode ser positiva ou negativa).
            data: Data do ajuste no formato `YYYY-MM-DD`.
        """
        banco_col = self._validar_nome_coluna_banco(banco_col)

        # garante existência da coluna
        cols = [r[1] for r in conn.execute("PRAGMA table_info(saldos_bancos)").fetchall()]
        if banco_col not in cols:
            # usar aspas duplas para quotar identificador
            conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_col}" REAL DEFAULT 0.0;')
            logger.debug("Criada coluna dinâmica em saldos_bancos: %s", banco_col)

        # garante a linha do dia
        self._garantir_linha_saldos_bancos(conn, data)

        # aplica o delta de forma parametrizada (valor) com identificador quotado
        conn.execute(
            f'UPDATE saldos_bancos SET "{banco_col}" = COALESCE("{banco_col}",0) + ? WHERE data = ?',
            (float(delta), data),
        )
        logger.debug("Ajustado banco_col=%s em %s com delta=%.2f", banco_col, data, float(delta))

    # ----------------------------------------------------------------------
    # data utils
    # ----------------------------------------------------------------------
    @staticmethod
    def _add_months(dt: date, months: int) -> date:
        """Soma meses preservando fim de mês quando aplicável.

        Args:
            dt: Data base.
            months: Número (positivo/negativo) de meses a somar.

        Returns:
            date: Nova data após o deslocamento.
        """
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
        """Calcula a competência da compra (regra de cartão).

        Regra:
            fechamento = date(vencimento) - dias_fechamento
            - Compra NO dia de fechamento -> competência do mês ATUAL
            - Compra DEPOIS do fechamento -> PRÓXIMO mês

        Args:
            compra_dt: Data/hora da compra.
            vencimento_dia: Dia do vencimento do cartão (1–31).
            dias_fechamento: Dias antes do vencimento que ocorre o fechamento.

        Returns:
            str: Competência no formato `YYYY-MM`.
        """
        y, m = compra_dt.year, compra_dt.month
        last = calendar.monthrange(y, m)[1]
        venc_d = min(int(vencimento_dia), last)

        # meia-noite do dia de vencimento (compensação por meses menores)
        venc_date = datetime(y, m, venc_d)
        fechamento_date = venc_date - timedelta(days=int(dias_fechamento))

        # No dia do fechamento permanece no mês atual; após, empurra para próximo mês
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


# --- Helpers padrão p/ lançamentos em 'movimentacoes_bancarias' ---

def _resolve_usuario(u: Any) -> str:
    """Aceita string, dict (nome/name/username/user/email/login) ou outros tipos e devolve uma string segura."""
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


def _ensure_mov_cols(cur) -> None:
    """Garante colunas 'usuario' e 'data_hora' em movimentacoes_bancarias (idempotente)."""
    cols = {row[1] for row in cur.execute("PRAGMA table_info(movimentacoes_bancarias);").fetchall()}
    if "usuario" not in cols:
        cur.execute("ALTER TABLE movimentacoes_bancarias ADD COLUMN usuario TEXT;")
    if "data_hora" not in cols:
        cur.execute("ALTER TABLE movimentacoes_bancarias ADD COLUMN data_hora TEXT;")


def log_mov_bancaria(
    conn,
    *,
    data: str,               # 'YYYY-MM-DD'
    banco: str,              # ex.: "Caixa 2"
    tipo: str,               # "entrada" | "saida"
    valor: float,
    origem: str,             # ex.: "transferencia_caixa"
    observacao: str,         # texto já padronizado (sem 'REF=...')
    usuario: Optional[Any] = None,
    referencia_id: Optional[int] = None,
    referencia_tabela: Optional[str] = None,
    trans_uid: Optional[str] = None,
    data_hora: Optional[str] = None,   # default = agora
    auto_self_reference: bool = True,  # se True e ref não for fornecida, faz self-reference
) -> int:
    """Insere linha padronizada no livro e retorna o id do movimento.
    Obs.: não dá commit; o caller decide quando commitar.
    """
    cur = conn.cursor()
    _ensure_mov_cols(cur)

    uid = trans_uid or str(uuid.uuid4())
    user = _resolve_usuario(usuario)
    dh = data_hora or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cur.execute(
        """
        INSERT INTO movimentacoes_bancarias
            (data, banco, tipo, valor, origem, observacao,
             referencia_id, referencia_tabela, trans_uid, usuario, data_hora)
        VALUES (?,    ?,     ?,    ?,     ?,      ?,
                ?,            ?,                ?,       ?,        ?)
        """,
        (data, banco, tipo, valor, origem, observacao,
         referencia_id, referencia_tabela, uid, user, dh),
    )
    mov_id = cur.lastrowid

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
