# ===================== Actions: Depósito =====================
"""
Ações de Depósito (Caixa 2 -> Banco).

Fluxo:
- UPSERT do snapshot diário em `saldos_caixas`;
- Débito priorizando `caixa2_dia` e depois `caixa_2`;
- 1 linha em `movimentacoes_bancarias` (self-reference via `referencia_id`);
- Atualiza `saldos_bancos` via `upsert_saldos_bancos`;
- (Opcional) Espelho em `depositos_bancarios`.

Mensagem final:
"✅ Depósito registrado em <Banco>: R$ X,XX | Origem → Caixa 2"
"""

from __future__ import annotations

import os
import uuid
from typing import List, Optional, Tuple, TypedDict

import pandas as pd

from shared.db import get_conn
from utils.utils import formatar_valor
from flowdash_pages.cadastros.cadastro_classes import BancoRepository
from flowdash_pages.lancamentos.shared_ui import canonicalizar_banco, upsert_saldos_bancos
from flowdash_pages.lancamentos.caixa2.actions_caixa2 import _ensure_snapshot_herdado


class ResultadoDeposito(TypedDict):
    """Estrutura de retorno de `registrar_deposito`."""
    ok: bool
    msg: str
    banco: str
    valor: float
    usar_de_dia: float
    usar_de_saldo: float


# ------------------------------- helpers básicos -------------------------------

def _r2(x) -> float:
    """Arredonda para 2 casas decimais evitando -0,00."""
    return round(float(x or 0.0), 2)


def _to_date_str(data_lanc) -> str:
    """Normaliza a data do lançamento para 'YYYY-MM-DD'."""
    d = pd.to_datetime(data_lanc, errors="coerce")
    if pd.isna(d):
        raise ValueError("Data de lançamento inválida.")
    return d.date().isoformat()


def _fmt_ptbr_valor(v: float) -> str:
    """Formata float em 'R$ X.XXX,YY' (padrão pt-BR) sem depender de locale."""
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def carregar_nomes_bancos(caminho_banco: str) -> List[str]:
    """Retorna a lista de nomes de bancos cadastrados."""
    df = BancoRepository(caminho_banco).carregar_bancos()
    return df["nome"].tolist() if df is not None and not df.empty else []


# ------------------------------- helpers de saldos_caixas -------------------------------

def _read_saldos_caixas(conn) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT id, data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total "
        "FROM saldos_caixas",
        conn,
    )


def _bases_para_data(df: pd.DataFrame, data_str: str) -> Tuple[Optional[int], float, float, float, float]:
    """
    Obtém snapshot do dia (se existir) ou o último anterior para bases.
    Retorna: (snap_id, base_caixa, base_caixa2, base_vendas, base_caixa2dia)
    """
    snap_id = None
    base_caixa = base_caixa2 = base_vendas = base_caixa2dia = 0.0
    if df.empty:
        return None, 0.0, 0.0, 0.0, 0.0

    df = df.copy()
    df["data"] = pd.to_datetime(df["data"], errors="coerce", dayfirst=True)
    alvo = pd.to_datetime(data_str)

    same_day = df[df["data"].dt.date == alvo.date()]
    if not same_day.empty:
        row = same_day.sort_values(["data", "id"]).tail(1).iloc[0]
        snap_id = int(row["id"])
        base_caixa = _r2(row.get("caixa", 0.0))
        base_caixa2 = _r2(row.get("caixa_2", 0.0))
        base_vendas = _r2(row.get("caixa_vendas", 0.0))
        base_caixa2dia = _r2(row.get("caixa2_dia", 0.0))
        return snap_id, base_caixa, base_caixa2, base_vendas, base_caixa2dia

    prev = df[df["data"].dt.date < alvo.date()]
    if not prev.empty:
        row = prev.sort_values(["data", "id"]).tail(1).iloc[0]
        base_caixa = _r2(row.get("caixa", 0.0))
        base_caixa2 = _r2(row.get("caixa_2", 0.0))
        base_vendas = _r2(row.get("caixa_vendas", 0.0))
        base_caixa2dia = 0.0  # novo dia → caixa2_dia começa em 0
    return snap_id, base_caixa, base_caixa2, base_vendas, base_caixa2dia


def _upsert_saldos_caixas(cur, *, snap_id: Optional[int], data_str: str,
                          novo_caixa: float, novo_caixa_2: float,
                          novo_caixa_vendas: float, novo_caixa_total: float,
                          novo_caixa2_dia: float, novo_caixa2_total: float) -> None:
    """Faz UPDATE se houver snapshot do dia; caso contrário, INSERT."""
    if snap_id is not None:
        cur.execute(
            """
            UPDATE saldos_caixas
               SET caixa=?, caixa_2=?, caixa_vendas=?, caixa_total=?, caixa2_dia=?, caixa2_total=?
             WHERE id=?
            """,
            (novo_caixa, novo_caixa_2, novo_caixa_vendas, novo_caixa_total,
             novo_caixa2_dia, novo_caixa2_total, snap_id),
        )
    else:
        cur.execute(
            """
            INSERT INTO saldos_caixas
                (data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total)
            VALUES (?,    ?,     ?,       ?,            ?,           ?,          ?)
            """,
            (data_str, novo_caixa, novo_caixa_2, novo_caixa_vendas,
             novo_caixa_total, novo_caixa2_dia, novo_caixa2_total),
        )


# ------------------------------- helpers de movimentações -------------------------------

def _insert_movimentacao(cur, *, data_str: str, data_hora_now: str, usuario: str,
                         banco_nome: str, valor_f: float) -> Tuple[int, str]:
    """
    Insere a linha em `movimentacoes_bancarias` e retorna (mov_id, trans_uid).
    Observação: "Lançamento DEPÓSITO Cx2→<Banco> | Valor=R$ X,XX"
    """
    trans_uid = str(uuid.uuid4())
    observ = f"Lançamento DEPÓSITO Cx2→{banco_nome} | Valor={_fmt_ptbr_valor(valor_f)}"
    cur.execute(
        """
        INSERT INTO movimentacoes_bancarias
            (data, data_hora, usuario, banco, tipo, valor, origem, observacao,
             referencia_id, referencia_tabela, trans_uid)
        VALUES (?,   ?,        ?,       ?,    'entrada', ?,    'deposito', ?,
                ?,            'movimentacoes_bancarias', ?)
        """,
        (data_str, data_hora_now, usuario, banco_nome, valor_f, observ, None, trans_uid),
    )
    mov_id = cur.lastrowid
    cur.execute("UPDATE movimentacoes_bancarias SET referencia_id=? WHERE id=?", (mov_id, mov_id))
    return mov_id, trans_uid


# ------------------------------- (opcional) tabela: depositos_bancarios -------------------------------

def ensure_tabela_depositos(cur) -> None:
    """Garante a existência de `depositos_bancarios` (idempotente)."""
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS depositos_bancarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            banco TEXT NOT NULL,
            valor REAL NOT NULL,
            origem TEXT NOT NULL,
            usar_de_dia REAL NOT NULL,
            usar_de_saldo REAL NOT NULL,
            mov_id INTEGER NOT NULL,
            trans_uid TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )


def insert_deposito_row(cur, *, data_str: str, banco: str, valor: float,
                        usar_de_dia: float, usar_de_saldo: float,
                        mov_id: int, trans_uid: str) -> None:
    """Insere (ou ignora por `trans_uid`) a linha em `depositos_bancarios`."""
    cur.execute(
        """
        INSERT OR IGNORE INTO depositos_bancarios
            (data, banco, valor, origem, usar_de_dia, usar_de_saldo, mov_id, trans_uid)
        VALUES (?,    ?,     ?,     'CAIXA_2', ?,           ?,            ?,      ?)
        """,
        (data_str, banco, valor, usar_de_dia, usar_de_saldo, mov_id, trans_uid),
    )


# ----------------------------- ação principal -----------------------------

def registrar_deposito(
    caminho_banco: str,
    data_lanc,
    valor: float,
    banco_in: str,
    usuario: Optional[str] = None,
) -> ResultadoDeposito:
    """
    Registra Depósito (Caixa 2 → Banco) e persiste nas tabelas correspondentes.

    Args:
        caminho_banco (str): Caminho do arquivo SQLite.
        data_lanc: Data do lançamento (qualquer formato aceito por `pandas.to_datetime`).
        valor (float): Valor do depósito (> 0).
        banco_in (str): Banco informado na UI (será canonicalizado).
        usuario (Optional[str]): Nome do usuário logado; se vazio, tenta `FLOWDASH_USER`,
            caso contrário usa 'sistema'.

    Returns:
        ResultadoDeposito: Dados para feedback na UI.

    Raises:
        ValueError: Valor/banco inválidos ou saldo insuficiente.
        RuntimeError: Falha ao atualizar `saldos_bancos`.
    """
    if valor is None or float(valor) <= 0:
        raise ValueError("Valor inválido.")
    banco_in = (banco_in or "").strip()
    if not banco_in:
        raise ValueError("Selecione ou digite o banco de destino.")

    usuario = (usuario if isinstance(usuario, str) else "").strip() or os.getenv("FLOWDASH_USER", "").strip() or "sistema"
    data_hora_now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    # Banco canonicalizado + normalizações
    try:
        banco_nome = canonicalizar_banco(caminho_banco, banco_in) or banco_in
    except Exception:
        banco_nome = banco_in
    data_str = _to_date_str(data_lanc)
    valor_f = _r2(valor)

    with get_conn(caminho_banco) as conn:
        # >>> garante snapshot herdado antes de validar o saldo
        _ensure_snapshot_herdado(conn, data_str)

        cur = conn.cursor()

        # Bases de saldos
        df_caixas = _read_saldos_caixas(conn)
        snap_id, base_caixa, base_caixa2, base_vendas, base_caixa2dia = _bases_para_data(df_caixas, data_str)

        # Validação de saldo (Caixa 2 do dia + saldo acumulado)
        base_total_cx2 = _r2(base_caixa2 + base_caixa2dia)
        if valor_f > base_total_cx2:
            raise ValueError(
                f"Valor indisponível no Caixa 2. Disponível: {formatar_valor(base_total_cx2)} "
                f"(Dia: {formatar_valor(base_caixa2dia)} • Saldo: {formatar_valor(base_caixa2)})"
            )

        # Quebra do débito (dia → saldo)
        usar_de_dia = _r2(min(valor_f, base_caixa2dia))
        usar_de_saldo = _r2(valor_f - usar_de_dia)

        novo_caixa2_dia = max(0.0, _r2(base_caixa2dia - usar_de_dia))
        novo_caixa_2 = max(0.0, _r2(base_caixa2 - usar_de_saldo))
        novo_caixa = base_caixa
        novo_caixa_vendas = base_vendas
        novo_caixa_total = _r2(novo_caixa + novo_caixa_vendas)
        novo_caixa2_total = _r2(novo_caixa_2 + novo_caixa2_dia)

        # UPSERT snapshot do dia
        _upsert_saldos_caixas(
            cur,
            snap_id=snap_id,
            data_str=data_str,
            novo_caixa=novo_caixa,
            novo_caixa_2=novo_caixa_2,
            novo_caixa_vendas=novo_caixa_vendas,
            novo_caixa_total=novo_caixa_total,
            novo_caixa2_dia=novo_caixa2_dia,
            novo_caixa2_total=novo_caixa2_total,
        )

        # Movimentação (self-reference)
        mov_id, trans_uid = _insert_movimentacao(
            cur,
            data_str=data_str,
            data_hora_now=data_hora_now,
            usuario=usuario,
            banco_nome=banco_nome,
            valor_f=valor_f,
        )

        # (Opcional) espelho em `depositos_bancarios`
        try:
            ensure_tabela_depositos(cur)
            insert_deposito_row(
                cur,
                data_str=data_str,
                banco=banco_nome,
                valor=valor_f,
                usar_de_dia=usar_de_dia,
                usar_de_saldo=usar_de_saldo,
                mov_id=mov_id,
                trans_uid=trans_uid,
            )
        except Exception:
            # Qualquer falha nessa tabela opcional não bloqueia o fluxo principal
            pass

        conn.commit()

    # Atualiza saldos_bancos (entrada no banco de destino)
    try:
        upsert_saldos_bancos(caminho_banco, data_str, banco_nome, valor_f)
    except Exception as e:
        raise RuntimeError(f"Não foi possível atualizar saldos_bancos para '{banco_nome}': {e}") from e

    # Mensagem final (padrão combinado)
    msg = f"✅ Depósito registrado em {banco_nome}: {_fmt_ptbr_valor(valor_f)} | Origem → Caixa 2"

    return {
        "ok": True,
        "msg": msg,
        "banco": banco_nome,
        "valor": valor_f,
        "usar_de_dia": usar_de_dia,
        "usar_de_saldo": usar_de_saldo,
    }
