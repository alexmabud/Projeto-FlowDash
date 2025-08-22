# ===================== Actions: Depósito =====================
"""
Executa a MESMA lógica/SQL do módulo original de Depósito:
- UPSERT diário em `saldos_caixas`
- Debita primeiro `caixa2_dia`, depois `caixa_2`
- Insere 1 linha em `movimentacoes_bancarias` (self-reference via referencia_id)
- Atualiza `saldos_bancos` via helper `upsert_saldos_bancos`
"""

import uuid
from typing import TypedDict
import pandas as pd

from shared.db import get_conn
from utils.utils import formatar_valor
from flowdash_pages.cadastros.cadastro_classes import BancoRepository
from flowdash_pages.lancamentos.shared_ui import upsert_saldos_bancos, canonicalizar_banco

class ResultadoDeposito(TypedDict):
    ok: bool
    msg: str
    banco: str
    valor: float
    usar_de_dia: float
    usar_de_saldo: float

def _r2(x) -> float:
    """Arredonda para 2 casas (evita -0,00)."""
    return round(float(x or 0.0), 2)

def carregar_nomes_bancos(caminho_banco: str) -> list[str]:
    """Obtém lista de nomes de bancos cadastrados."""
    repo = BancoRepository(caminho_banco)
    df = repo.carregar_bancos()
    return df["nome"].tolist() if df is not None and not df.empty else []

def registrar_deposito(caminho_banco: str, data_lanc, valor: float, banco_in: str) -> ResultadoDeposito:
    """
    Registra o depósito (do Caixa 2 para um banco).

    Args:
        caminho_banco: Caminho do SQLite.
        data_lanc: Data (date/str).
        valor: Valor a depositar (>0).
        banco_in: Nome do banco de destino (será canonicalizado).

    Returns:
        ResultadoDeposito: dados para exibição.

    Raises:
        ValueError: validações de valor/banco/saldo.
        Exception: erros de banco/SQL.
    """
    if valor is None or float(valor) <= 0:
        raise ValueError("Valor inválido.")
    banco_in = (banco_in or "").strip()
    if not banco_in:
        raise ValueError("Selecione ou digite o banco de destino.")

    # Canonicalizar banco (mantendo comportamento original)
    try:
        banco_nome = canonicalizar_banco(caminho_banco, banco_in) or banco_in
    except Exception:
        banco_nome = banco_in

    data_str = str(data_lanc)
    valor_f = _r2(valor)

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # ================== SNAPSHOT DO DIA EM saldos_caixas (UPSERT) ==================
        df_caixas = pd.read_sql(
            "SELECT id, data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total FROM saldos_caixas",
            conn
        )
        snap_id = None
        base_caixa = base_caixa2 = base_vendas = base_caixa2dia = 0.0

        if not df_caixas.empty:
            df_caixas["data"] = pd.to_datetime(df_caixas["data"], errors="coerce", dayfirst=True)
            same_day = df_caixas[df_caixas["data"].dt.date == pd.to_datetime(data_lanc).date()]
            if not same_day.empty:
                same_day = same_day.sort_values(["data","id"]).tail(1)
                snap_id        = int(same_day.iloc[0]["id"])
                base_caixa     = _r2(same_day.iloc[0].get("caixa", 0.0))
                base_caixa2    = _r2(same_day.iloc[0].get("caixa_2", 0.0))
                base_vendas    = _r2(same_day.iloc[0].get("caixa_vendas", 0.0))
                base_caixa2dia = _r2(same_day.iloc[0].get("caixa2_dia", 0.0))
            else:
                prev = df_caixas[df_caixas["data"].dt.date < pd.to_datetime(data_lanc).date()]
                if not prev.empty:
                    prev = prev.sort_values(["data","id"]).tail(1)
                    base_caixa     = _r2(prev.iloc[0].get("caixa", 0.0))
                    base_caixa2    = _r2(prev.iloc[0].get("caixa_2", 0.0))
                    base_vendas    = _r2(prev.iloc[0].get("caixa_vendas", 0.0))
                    base_caixa2dia = 0.0  # novo dia começa com 0 no caixa2_dia

        base_total_cx2 = _r2(base_caixa2 + base_caixa2dia)
        if valor_f > base_total_cx2:
            raise ValueError(
                f"Valor indisponível no Caixa 2. Disponível: {formatar_valor(base_total_cx2)} "
                f"(Dia: {formatar_valor(base_caixa2dia)} • Saldo: {formatar_valor(base_caixa2)})"
            )

        # Debita primeiro do dia, depois do saldo
        usar_de_dia   = _r2(min(valor_f, base_caixa2dia))
        usar_de_saldo = _r2(valor_f - usar_de_dia)

        novo_caixa2_dia  = max(0.0, _r2(base_caixa2dia - usar_de_dia))
        novo_caixa_2     = max(0.0, _r2(base_caixa2 - usar_de_saldo))
        # Caixa físico não muda no depósito (vai do Caixa 2 pro banco)
        novo_caixa        = base_caixa
        novo_caixa_vendas = base_vendas
        novo_caixa_total  = _r2(novo_caixa + novo_caixa_vendas)
        novo_caixa2_total = _r2(novo_caixa_2 + novo_caixa2_dia)

        if snap_id is not None:
            cur.execute("""
                UPDATE saldos_caixas
                   SET caixa=?,
                       caixa_2=?,
                       caixa_vendas=?,
                       caixa_total=?,
                       caixa2_dia=?,
                       caixa2_total=?
                 WHERE id=?
            """, (novo_caixa, novo_caixa_2, novo_caixa_vendas, novo_caixa_total,
                  novo_caixa2_dia, novo_caixa2_total, snap_id))
        else:
            cur.execute("""
                INSERT INTO saldos_caixas
                    (data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total)
                VALUES (?,    ?,     ?,       ?,            ?,           ?,          ?)
            """, (data_str, novo_caixa, novo_caixa_2, novo_caixa_vendas,
                  novo_caixa_total, novo_caixa2_dia, novo_caixa2_total))

        # ================== LIVRO: **uma linha por lançamento** ==================
        trans_uid = str(uuid.uuid4())
        observ = (
            f"Depósito Cx2→{banco_nome} | "
            f"Valor={formatar_valor(valor_f)} | "
            f"dia={usar_de_dia:.2f}; saldo={usar_de_saldo:.2f}"
        )
        cur.execute("""
            INSERT INTO movimentacoes_bancarias
                (data, banco,   tipo,     valor,  origem,   observacao,
                 referencia_id, referencia_tabela, trans_uid)
            VALUES (?,   ?,      ?,        ?,      ?,        ?,
                    ?,             ?,                 ?)
        """, (
            data_str, banco_nome, "entrada", valor_f, "deposito", observ,
            None, "movimentacoes_bancarias", trans_uid
        ))
        mov_id = cur.lastrowid

        observ_final = observ + f" | REF={mov_id}"
        cur.execute("""
            UPDATE movimentacoes_bancarias
               SET referencia_id = ?, observacao = ?
             WHERE id = ?
        """, (mov_id, observ_final, mov_id))

        conn.commit()

    # ================== saldos_bancos (helper padronizado) ==================
    try:
        upsert_saldos_bancos(caminho_banco, data_str, banco_nome, valor_f)
    except Exception as e:
        # Não interrompe a operação, replica aviso do original (UI avisará).
        raise RuntimeError(f"Não foi possível atualizar saldos_bancos para '{banco_nome}': {e}") from e

    return {
        "ok": True,
        "msg": (
            f"✅ Depósito registrado em {banco_nome}: {formatar_valor(valor_f)} "
            f"(abatido do Caixa 2 — Dia: {formatar_valor(usar_de_dia)}, Saldo: {formatar_valor(usar_de_saldo)})."
        ),
        "banco": banco_nome,
        "valor": valor_f,
        "usar_de_dia": usar_de_dia,
        "usar_de_saldo": usar_de_saldo,
    }