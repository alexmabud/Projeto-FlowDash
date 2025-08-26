# ===================== Actions: Caixa 2 =====================
"""
Actions: Caixa 2

Resumo:
    Executa a transferência (Caixa/Caixa Vendas → Caixa 2), atualiza o snapshot diário
    em `saldos_caixas` e registra 1 (uma) linha no livro `movimentacoes_bancarias`
    usando o helper padronizado `log_mov_bancaria`.

Mantém:
    - Abatimento prioritário do saldo: primeiro `caixa`, depois `caixa_vendas`.
    - Snapshot/UPSERT único por dia em `saldos_caixas`.
    - Observação padronizada SEM "REF=...":
      "Lançamento Transferência p/ Caixa 2 | Valor=R$ X | C=R$ Y; CV=R$ Z".
    - Self-reference automática em `movimentacoes_bancarias` (referencia_id/tabela).

Entrada:
    - caminho_banco (str): caminho do arquivo SQLite.
    - data_lanc (date | str 'YYYY-MM-DD'): data do lançamento.
    - valor (float > 0): valor a transferir.
    - usuario (str | dict | None): quem executou (aceita dict com 'nome'/'name'/'username'/...).

Saída:
    - ResultadoTransferencia (TypedDict): {ok, msg, valor, usar_caixa, usar_vendas}.

Notas:
    - Commit é realizado ao final da operação.
    - Formatação monetária BRL consistente entre UI e livro.
"""

from __future__ import annotations

from typing import TypedDict, Any, Optional

from shared.db import get_conn
from services.ledger.service_ledger_infra import log_mov_bancaria, _resolve_usuario


class ResultadoTransferencia(TypedDict):
    ok: bool
    msg: str
    valor: float
    usar_caixa: float
    usar_vendas: float


def _r2(x: Any) -> float:
    """Arredonda para 2 casas, tolerando None/str."""
    try:
        return round(float(x or 0.0), 2)
    except Exception:
        return 0.0


def fmt_brl(x: float) -> str:
    """Formata BRL para observação/armazenamento (ex.: 'R$ 1.234,56')."""
    s = f"{float(x):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"R$ {s}"


def fmt_brl_md(x: float) -> str:
    """Formata BRL para mensagens em Streamlit (escapa '$'): 'R\\$ 1.234,56'."""
    s = f"{float(x):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"R\\$ {s}"


def transferir_para_caixa2(
    caminho_banco: str,
    data_lanc,
    valor: float,
    usuario: Optional[Any] = None,  # aceita str ou dict
) -> ResultadoTransferencia:
    """
    Transfere recursos de `caixa`/`caixa_vendas` para o `caixa_2` (Caixa 2).

    Processo:
        1) Resolve snapshot do dia em `saldos_caixas` (ou baseia-se no último < data).
        2) Valida saldo disponível (caixa + caixa_vendas).
        3) Abate primeiro de `caixa`, depois de `caixa_vendas`.
        4) Faz UPSERT do snapshot do dia.
        5) Registra 1 (uma) linha de ENTRADA em `movimentacoes_bancarias`
           usando `log_mov_bancaria`, com self-reference automática.

    Args:
        caminho_banco: Caminho do arquivo SQLite.
        data_lanc: Data do lançamento (date ou 'YYYY-MM-DD').
        valor: Valor a transferir (> 0).
        usuario: Identificação de quem realizou (str ou dict com 'nome'/'name'/'username'/...).

    Returns:
        ResultadoTransferencia: Campos para a UI (ok, msg, valor, usar_caixa, usar_vendas).

    Raises:
        ValueError: Se `valor <= 0` ou se o saldo disponível for insuficiente.
        Exception: Para erros inesperados de banco/SQL.
    """
    if valor is None or float(valor) <= 0:
        raise ValueError("Valor inválido.")

    data_str = str(data_lanc)  # YYYY-MM-DD
    valor_f = _r2(valor)
    usuario_norm = _resolve_usuario(usuario)

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # --- Snapshot do DIA (ou base no último < data)
        same = cur.execute(
            """
            SELECT id, data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
              FROM saldos_caixas
             WHERE date(data)=?
          ORDER BY id DESC
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
                 WHERE date(data) < ?
              ORDER BY date(data) DESC, id DESC
                 LIMIT 1
                """,
                (data_str,),
            ).fetchone()
            snap_id        = None
            base_caixa     = _r2(prev[1]) if prev else 0.0
            base_caixa2    = _r2(prev[2]) if prev else 0.0
            base_vendas    = _r2(prev[3]) if prev else 0.0
            base_caixa2dia = 0.0  # novo dia

        base_total_dinheiro = _r2(base_caixa + base_vendas)
        if valor_f > base_total_dinheiro:
            raise ValueError(f"Valor indisponível. Caixa Total atual é {fmt_brl_md(base_total_dinheiro)}.")

        # --- Abatimento: primeiro Caixa, depois Caixa Vendas
        usar_caixa  = _r2(min(valor_f, base_caixa))
        usar_vendas = _r2(valor_f - usar_caixa)

        novo_caixa       = max(0.0, _r2(base_caixa - usar_caixa))
        novo_vendas      = max(0.0, _r2(base_vendas - usar_vendas))
        novo_caixa2_dia  = max(0.0, _r2(base_caixa2dia + valor_f))
        novo_caixa_total = _r2(novo_caixa + novo_vendas)
        novo_caixa2_tot  = _r2(base_caixa2 + novo_caixa2_dia)  # acumulado

        # --- UPSERT snapshot do dia
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
                VALUES (?,    ?,     ?,       ?,            ?,           ?,          ?)
                """,
                (data_str, novo_caixa, base_caixa2, novo_vendas,
                 novo_caixa_total, novo_caixa2_dia, novo_caixa2_tot),
            )

        # --- Livro (1 linha, entrada em Caixa 2) via helper
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
            observacao=observ,        # sem REF=...
            usuario=usuario_norm,     # string já resolvida
            # referencia_id=None / referencia_tabela=None -> self-reference automática
            # trans_uid=None -> helper gera automaticamente
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
