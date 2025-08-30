"""
Infraestrutura do Ledger.

Utilitários comuns para serviços do Ledger:
- Garantir linhas em `saldos_caixas` e `saldos_bancos`.
- Criar/ajustar colunas dinâmicas de bancos de forma segura.
- Helpers de data (somar meses preservando fim de mês; competência de cartão).
- Helper para padronizar a coluna `observacao` (saídas).
- Helper para registrar linhas padronizadas em `movimentacoes_bancarias`.

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

__all__ = ["_InfraLedgerMixin", "log_mov_bancaria", "_fmt_obs_saida"]


class _InfraLedgerMixin:
    """Mixin com utilitários de infraestrutura para o Ledger."""

    # ------------------------------------------------------------------
    # saldos_caixas / saldos_bancos
    # ------------------------------------------------------------------
    def _garantir_linha_saldos_caixas(self, conn: sqlite3.Connection, data: str) -> None:
        """Garante a existência da linha em `saldos_caixas` para a data.

        Args:
            conn (sqlite3.Connection): Conexão ativa com o banco SQLite.
            data (str): Data alvo no formato 'YYYY-MM-DD'.
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
            conn (sqlite3.Connection): Conexão ativa com o banco SQLite.
            data (str): Data alvo no formato 'YYYY-MM-DD'.
        """
        cur = conn.execute("SELECT 1 FROM saldos_bancos WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("INSERT OR IGNORE INTO saldos_bancos (data) VALUES (?)", (data,))
            logger.debug("Criada linha em saldos_bancos para data=%s", data)

    # --------- validação segura de nome de coluna (bancos dinâmicos) --------
    _COL_RE = re.compile(r"^[A-Za-z0-9_ ]{1,64}$")  # letras, números, underscore e espaço

    def _validar_nome_coluna_banco(self, banco_col: str) -> str:
        """Valida o nome da coluna de banco (whitelist) e retorna a versão segura.

        Args:
            banco_col (str): Nome da coluna a validar.

        Returns:
            str: Nome validado.

        Raises:
            ValueError: Se o nome não respeitar o padrão permitido.
        """
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

        Args:
            conn (sqlite3.Connection): Conexão ativa com o banco SQLite.
            banco_col (str): Nome da coluna (validação por whitelist).
            delta (float): Variação a ser aplicada (positiva/negativa).
            data (str): Data alvo no formato 'YYYY-MM-DD'.
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
        """Soma meses preservando fim de mês quando aplicável.

        Args:
            dt (date): Data base.
            months (int): Quantidade de meses a somar (positivo/negativo).

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
        """Calcula a competência contábil da compra (regra de cartão).

        A competência muda para o mês seguinte se a compra ocorrer APÓS a data
        de fechamento (vencimento - dias_fechamento).

        Args:
            compra_dt (datetime): Momento da compra.
            vencimento_dia (int): Dia do vencimento da fatura (1..31).
            dias_fechamento (int): Dias de fechamento antes do vencimento.

        Returns:
            str: Competência no formato 'YYYY-MM'.
        """
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
    """Remove acentos (normalização simples p/ 'DEBITO' e 'CREDITO').

    Args:
        s (str): Texto de entrada.

    Returns:
        str: Texto sem acentos.
    """
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
    """Padroniza a `observacao` para `movimentacoes_bancarias` em SAÍDAS.

    Formatos:
        - DINHEIRO: "Lançamento SAÍDA DINHEIRO R$<valor> • <Categoria>, <Sub Categoria>, • <descrição opcional>"
        - PIX:      "Lançamento SAÍDA PIX R$<valor> • <Categoria>, <Sub Categoria>, • <descrição opcional>"
        - DEBITO:   "Lançamento SAÍDA DEBITO <Banco> R$<valor> • <Categoria>, <Sub Categoria>, • <descrição opcional>"
        - CREDITO:  "Lançamento SAÍDA CREDITO <Cartão> R$<valor> • <Categoria>, <Sub Categoria>, • <descrição opcional> • Nx"
        - BOLETO:   "Lançamento SAÍDA BOLETO R$<valor> • <Categoria>, <Sub Categoria>, • <descrição opcional>"

    Regras:
        - Nunca usar parênteses; manter bullets com '•'.
        - 'DEBITO'/'CREDITO' sempre sem acento.
        - Valor com duas casas decimais e sem espaço entre 'R$' e o número (ex.: R$123.45).
        - O sufixo " • Nx" aparece **apenas** para CREDITO e quando `parcelas >= 2`.

    Args:
        forma (str): Forma da saída (DINHEIRO/PIX/DEBITO/CREDITO/BOLETO).
        valor (float): Valor da saída.
        categoria (str | None): Categoria (texto livre).
        subcategoria (str | None): Subcategoria (texto livre).
        descricao (str | None): Descrição opcional.
        banco (str | None): Nome do banco (usado em DEBITO).
        cartao (str | None): Nome do cartão (usado em CREDITO).
        parcelas (int | None): Quantidade de parcelas (CREDITO; sufixo exibido se >= 2).

    Returns:
        str: Observação padronizada.
    """
    f_raw = (forma or "").strip().upper()
    f = _sem_acentos(f_raw)
    if f == "DEBITO A VISTA":
        f = "DEBITO"
    elif f in {"CARTAO", "CARTAO DE CREDITO"}:
        f = "CREDITO"

    validos = {"DINHEIRO", "PIX", "DEBITO", "CREDITO", "BOLETO"}
    if f not in validos:
        f = _sem_acentos(f_raw) or "SAIDA"

    # cabeçalho
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

    # valor
    try:
        vtxt = f"R${float(valor):.2f}"
    except Exception:
        vtxt = "R$0.00"

    # trilha de categoria/sub/descrição (sem parênteses)
    cat = (categoria or "").strip() or "-"
    sub = (subcategoria or "").strip() or "-"
    desc = (descricao or "").strip()

    trilha = f" • {cat}, {sub}"
    if desc:
        trilha += f", • {desc}"

    # sufixo de parcelas — SOMENTE para CREDITO e quando >= 2
    if f == "CREDITO" and isinstance(parcelas, int) and parcelas >= 2:
        trilha += f" • {parcelas}x"

    return f"{head} {vtxt}{trilha}"


def _resolve_usuario(u: Any) -> str:
    """Normaliza a informação de usuário em string segura.

    Aceita `str` ou `dict` com chaves usuais (nome/name/username/user/email/login).

    Args:
        u (Any): Valor representando o usuário.

    Returns:
        str: Nome do usuário (ou 'sistema' se não identificado).
    """
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
    """Garante colunas `usuario` e `data_hora` em `movimentacoes_bancarias` (idempotente).

    Args:
        cur (sqlite3.Cursor): Cursor apontando para o banco atual.
    """
    cols = {row[1] for row in cur.execute("PRAGMA table_info(movimentacoes_bancarias);").fetchall()}
    if "usuario" not in cols:
        cur.execute("ALTER TABLE movimentacoes_bancarias ADD COLUMN usuario TEXT;")
    if "data_hora" not in cols:
        cur.execute("ALTER TABLE movimentacoes_bancarias ADD COLUMN data_hora TEXT;")


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
    """Insere linha padronizada em `movimentacoes_bancarias` e retorna o id.

    Também garante as colunas `usuario` e `data_hora` (idempotente). Se
    `auto_self_reference=True` e `referencia_id` não for informado, a função
    referencia a própria linha inserida.

    Args:
        conn (sqlite3.Connection): Conexão ativa com o banco SQLite.
        data (str): Data do movimento em 'YYYY-MM-DD'.
        banco (str): Banco/coluna associada (texto livre do livro).
        tipo (str): Tipo do movimento (ex.: 'entrada' | 'saida' | 'transferencia').
        valor (float): Valor do movimento.
        origem (str): Origem (ex.: 'saidas', 'entradas', 'caixa2', etc.).
        observacao (str): Observação pronta para exibição.
        usuario (Optional[Any]): Usuário (str/dict); será normalizado.
        referencia_id (Optional[int]): Id de referência (ledger/saida/entrada/etc.).
        referencia_tabela (Optional[str]): Tabela de referência.
        trans_uid (Optional[str]): UID idempotente (se None, gera um UUID4).
        data_hora (Optional[str]): Data/hora no formato 'YYYY-MM-DD HH:MM:SS' (default: now).
        auto_self_reference (bool): Se True, referencia a própria linha quando `referencia_id` é None.

    Returns:
        int: ID do movimento inserido.
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
