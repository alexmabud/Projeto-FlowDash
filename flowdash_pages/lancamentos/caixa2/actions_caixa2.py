# ===================== Actions: Caixa 2 =====================
"""
Resumo
------
Executa a transferência (Caixa/Caixa Vendas → Caixa 2), atualiza o snapshot
diário em `saldos_caixas` e registra 1 (uma) linha no livro
`movimentacoes_bancarias`.

Decisões/Convenções
-------------------
- Abatimento prioritário do saldo: primeiro `caixa`, depois `caixa_vendas`.
- Snapshot único por dia em `saldos_caixas`:
    - caixa        ← prev.caixa
    - caixa_vendas ← prev.caixa_vendas
    - caixa_total  ← prev.caixa_total
    - caixa_2      ← prev.caixa2_total (saldo base do dia)
    - caixa2_dia   ← 0.0 (novo dia começa zerado)
    - caixa2_total ← prev.caixa2_total
- Observação padronizada (sem "REF="):
    "Lançamento Transferência p/ Caixa 2 | Valor=R$ X | C=R$ Y; CV=R$ Z".
- Self-reference automática em `movimentacoes_bancarias`
  (`referencia_id` / `trans_uid`).

Retorno
-------
TypedDict ResultadoTransferencia:
    ok, msg, valor, usar_caixa, usar_vendas
"""

from __future__ import annotations

import sqlite3
from typing import TypedDict, Any, Optional

from shared.db import get_conn
from services.ledger.service_ledger_infra import log_mov_bancaria, _resolve_usuario

__all__ = ["transferir_para_caixa2", "_ensure_snapshot_herdado"]


# ===================== Tipos =====================
class ResultadoTransferencia(TypedDict):
    ok: bool
    msg: str
    valor: float
    usar_caixa: float
    usar_vendas: float


# ===================== Helpers =====================
def _r2(x: Any) -> float:
    """Arredonda para 2 casas, tolerando None/str e evitando -0.00."""
    try:
        return round(float(x or 0.0), 2)
    except Exception:
        return 0.0


def fmt_brl(x: float) -> str:
    """Formata BRL para observação: 'R$ 1.234,56'."""
    s = f"{float(x):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"R$ {s}"


def fmt_brl_md(x: float) -> str:
    """Formata BRL para mensagens em Streamlit: 'R\\$ 1.234,56'."""
    s = f"{float(x):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"R\\$ {s}"


# ===================== API =====================
def transferir_para_caixa2(
    caminho_banco: str,
    data_lanc,
    valor: float,
    usuario: Optional[Any] = None,
) -> ResultadoTransferencia:
    """
    Transfere recursos de `caixa`/`caixa_vendas` para o `caixa_2`.

    Processo:
      1. Garante snapshot do dia (herdando do último dia anterior, se preciso).
      2. Valida saldo disponível (caixa + caixa_vendas).
      3. Abate primeiro de `caixa`, depois de `caixa_vendas`.
      4. Faz UPSERT do snapshot do dia.
      5. Registra 1 linha de ENTRADA em `movimentacoes_bancarias`.

    Raises:
        ValueError: Valor inválido ou saldo insuficiente.
        Exception: Erros inesperados de banco/SQL.
    """
    if valor is None or float(valor) <= 0:
        raise ValueError("Valor inválido.")

    data_str = str(data_lanc)  # YYYY-MM-DD
    valor_f = _r2(valor)
    usuario_norm = _resolve_usuario(usuario)

    with get_conn(caminho_banco) as conn:
        # Garante snapshot herdado para o dia
        _ensure_snapshot_herdado(conn, data_str)

        cur = conn.cursor()

        # Snapshot do dia (prefere linha não zerada se houver duplicadas)
        same = cur.execute(
            """
            SELECT id, data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
              FROM saldos_caixas
             WHERE DATE(data) = DATE(?)
          ORDER BY
               (COALESCE(caixa,0.0)
              + COALESCE(caixa_vendas,0.0)
              + COALESCE(caixa_total,0.0)
              + COALESCE(caixa2_dia,0.0)
              + COALESCE(caixa2_total,0.0)) DESC,
               id DESC
             LIMIT 1
            """,
            (data_str,),
        ).fetchone()

        if same:
            snap_id        = same[0]
            base_caixa     = _r2(same[2])
            base_caixa2    = _r2(same[3])
            base_vendas    = _r2(same[4])
            base_caixa2dia = _r2(same[6])
        else:
            prev = cur.execute(
                """
                SELECT data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
                  FROM saldos_caixas
                 WHERE DATE(data) < DATE(?)
              ORDER BY DATE(data) DESC, id DESC
                 LIMIT 1
                """,
                (data_str,),
            ).fetchone()
            snap_id        = None
            base_caixa     = _r2(prev[1]) if prev else 0.0
            base_caixa2    = _r2(prev[2]) if prev else 0.0
            base_vendas    = _r2(prev[3]) if prev else 0.0
            base_caixa2dia = 0.0  # novo dia

        # Valida disponibilidade (somente caixa + vendas)
        base_total_dinheiro = _r2(base_caixa + base_vendas)
        if valor_f > base_total_dinheiro:
            raise ValueError(
                f"Valor indisponível. Caixa Total atual é {fmt_brl_md(base_total_dinheiro)}."
            )

        # Abatimento
        usar_caixa  = _r2(min(valor_f, base_caixa))
        usar_vendas = _r2(valor_f - usar_caixa)

        novo_caixa       = max(0.0, _r2(base_caixa - usar_caixa))
        novo_vendas      = max(0.0, _r2(base_vendas - usar_vendas))
        novo_caixa2_dia  = max(0.0, _r2(base_caixa2dia + valor_f))
        novo_caixa_total = _r2(novo_caixa + novo_vendas)
        novo_caixa2_tot  = _r2(base_caixa2 + novo_caixa2_dia)

        # UPSERT snapshot
        if snap_id is not None:
            cur.execute(
                """
                UPDATE saldos_caixas
                   SET caixa=?,
                       caixa_vendas=?,
                       caixa_total=?,
                       caixa2_dia=?,
                       caixa2_total=?
                 WHERE id=?
                """,
                (novo_caixa, novo_vendas, novo_caixa_total,
                 novo_caixa2_dia, novo_caixa2_tot, snap_id),
            )
        else:
            cur.execute(
                """
                INSERT INTO saldos_caixas
                    (data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (data_str, novo_caixa, base_caixa2, novo_vendas,
                 novo_caixa_total, novo_caixa2_dia, novo_caixa2_tot),
            )

        # Livro (1 linha, entrada em Caixa 2)
        observ = (
            "Lançamento Transferência p/ Caixa 2 | "
            f"Valor={fmt_brl(valor_f)} | "
            f"C={fmt_brl(usar_caixa)}; CV={fmt_brl(usar_vendas)}"
        )
        log_mov_bancaria(
            conn,
            data=data_str,
            banco="Caixa 2",
            tipo="entrada",
            valor=valor_f,
            origem="transferencia_caixa",
            observacao=observ,
            usuario=usuario_norm,
        )

        conn.commit()

    return {
        "ok": True,
        "msg": (
            "✅ Transferência para Caixa 2 registrada: "
            f"{fmt_brl_md(valor_f)} | "
            f"Origem → Caixa: {fmt_brl_md(usar_caixa)}, "
            f"Caixa Vendas: {fmt_brl_md(usar_vendas)}"
        ),
        "valor": valor_f,
        "usar_caixa": usar_caixa,
        "usar_vendas": usar_vendas,
    }


# ===================== Snapshot diário =====================
def _ensure_snapshot_herdado(conn: sqlite3.Connection, data_str: str) -> None:
    """
    Garante snapshot do dia herdando do último anterior.

    Herdado:
      - caixa        ← prev.caixa
      - caixa_vendas ← prev.caixa_vendas
      - caixa_total  ← prev.caixa_total
      - caixa_2      ← prev.caixa2_total
      - caixa2_dia   ← 0.0
      - caixa2_total ← prev.caixa2_total

    Se já existir a data e estiver "zerada", atualiza para esse estado.
    """
    data_str = str(data_str)

    # Inserir se não existir
    sql_insert = """
    WITH prev AS (
        SELECT
            COALESCE(caixa, 0.0)        AS c,
            COALESCE(caixa_vendas, 0.0) AS cv,
            COALESCE(caixa_total, 0.0)  AS ct,
            COALESCE(caixa2_total, 0.0) AS c2t
        FROM saldos_caixas
        WHERE DATE(data) = (
            SELECT MAX(DATE(data))
              FROM saldos_caixas
             WHERE DATE(data) < DATE(:data)
        )
        LIMIT 1
    )
    INSERT INTO saldos_caixas
        (data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total)
    SELECT DATE(:data),
           COALESCE(c,0.0),
           COALESCE(c2t,0.0),
           COALESCE(cv,0.0),
           COALESCE(ct,0.0),
           0.0,
           COALESCE(c2t,0.0)
    FROM prev
    WHERE NOT EXISTS (
        SELECT 1 FROM saldos_caixas WHERE DATE(data)=DATE(:data)
    );
    """
    conn.execute(sql_insert, {"data": data_str})

    # Atualizar se já existe e está zerado
    sql_update = """
    WITH prev AS (
        SELECT
            COALESCE(caixa, 0.0)        AS c,
            COALESCE(caixa_vendas, 0.0) AS cv,
            COALESCE(caixa_total, 0.0)  AS ct,
            COALESCE(caixa2_total, 0.0) AS c2t
        FROM saldos_caixas
        WHERE DATE(data) = (
            SELECT MAX(DATE(data))
              FROM saldos_caixas
             WHERE DATE(data) < DATE(:data)
        )
        LIMIT 1
    )
    UPDATE saldos_caixas
       SET caixa        = COALESCE((SELECT c   FROM prev), 0.0),
           caixa_2      = COALESCE((SELECT c2t FROM prev), 0.0),
           caixa_vendas = COALESCE((SELECT cv  FROM prev), 0.0),
           caixa_total  = COALESCE((SELECT ct  FROM prev), 0.0),
           caixa2_dia   = 0.0,
           caixa2_total = COALESCE((SELECT c2t FROM prev), 0.0)
     WHERE DATE(data) = DATE(:data)
       AND COALESCE(caixa,0.0)=0.0
       AND COALESCE(caixa_vendas,0.0)=0.0
       AND COALESCE(caixa_total,0.0)=0.0
       AND COALESCE(caixa2_dia,0.0)=0.0;
    """
    conn.execute(sql_update, {"data": data_str})
