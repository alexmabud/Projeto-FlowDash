# ===================== Actions: Caixa 2 =====================
"""
Ações que executam a mesma lógica/SQL do módulo original de Caixa 2.

Mantém:
- snapshot/UPSERT diário em `saldos_caixas`
- abatimento prioritário de `caixa`, depois `caixa_vendas`
- registro no "livro" `movimentacoes_bancarias` com `trans_uid` e self-reference
"""

import uuid
from typing import TypedDict

from shared.db import get_conn
from utils import formatar_valor  # ← padronizado

class ResultadoTransferencia(TypedDict):
    ok: bool
    msg: str
    valor: float
    usar_caixa: float
    usar_vendas: float

def _r2(x) -> float:
    """Arredonda para 2 casas, tolerando None."""
    return round(float(x or 0.0), 2)

def transferir_para_caixa2(caminho_banco: str, data_lanc, valor: float) -> ResultadoTransferencia:
    """
    Executa a transferência para o Caixa 2.

    Args:
        caminho_banco: Caminho do arquivo SQLite.
        data_lanc: Data do lançamento (date ou str YYYY-MM-DD).
        valor: Valor a transferir (> 0).

    Returns:
        ResultadoTransferencia: dados da operação para exibir na UI.

    Raises:
        ValueError: Se valor inválido ou saldo indisponível.
        Exception: Para erros de banco/SQL.
    """
    if valor is None or float(valor) <= 0:
        raise ValueError("Valor inválido.")

    data_str = str(data_lanc)  # ISO YYYY-MM-DD (facilita filtros)
    valor_f = _r2(valor)

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # 1) Snapshot do DIA (se não houver, baseia no último < data)
        same = cur.execute("""
            SELECT id, data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
              FROM saldos_caixas
             WHERE date(data)=?
          ORDER BY id DESC
             LIMIT 1
        """, (data_str,)).fetchone()

        if same:
            snap_id        = same[0]
            base_caixa     = _r2(same[2])
            base_caixa2    = _r2(same[3])   # coluna: caixa_2 (saldo acumulado do Caixa 2)
            base_vendas    = _r2(same[4])
            base_caixa2dia = _r2(same[6])   # valor movimentado no dia para Caixa 2
        else:
            prev = cur.execute("""
                SELECT data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
                  FROM saldos_caixas
                 WHERE date(data) < ?
              ORDER BY date(data) DESC, id DESC
                 LIMIT 1
            """, (data_str,)).fetchone()
            snap_id        = None
            base_caixa     = _r2(prev[1]) if prev else 0.0
            base_caixa2    = _r2(prev[2]) if prev else 0.0
            base_vendas    = _r2(prev[3]) if prev else 0.0
            base_caixa2dia = 0.0  # novo dia começa em 0

        base_total_dinheiro = _r2(base_caixa + base_vendas)
        if valor_f > base_total_dinheiro:
            raise ValueError(f"Valor indisponível. Caixa Total atual é {formatar_valor(base_total_dinheiro)}.")

        # 2) Abate primeiro de 'caixa', depois de 'caixa_vendas'
        usar_caixa  = _r2(min(valor_f, base_caixa))
        usar_vendas = _r2(valor_f - usar_caixa)

        novo_caixa       = max(0.0, _r2(base_caixa - usar_caixa))
        novo_vendas      = max(0.0, _r2(base_vendas - usar_vendas))
        novo_caixa2_dia  = max(0.0, _r2(base_caixa2dia + valor_f))
        novo_caixa_total = _r2(novo_caixa + novo_vendas)
        novo_caixa2_tot  = _r2(base_caixa2 + novo_caixa2_dia)  # caixa_2 é a base, dia é o movimento

        # 3) UPSERT no snapshot do dia (uma linha por dia)
        if snap_id is not None:
            cur.execute("""
                UPDATE saldos_caixas
                   SET caixa=?,
                       caixa_vendas=?,
                       caixa_total=?,
                       caixa2_dia=?,
                       caixa2_total=?
                 WHERE id=?
            """, (novo_caixa, novo_vendas, novo_caixa_total,
                  novo_caixa2_dia, novo_caixa2_tot, snap_id))
        else:
            cur.execute("""
                INSERT INTO saldos_caixas
                    (data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total)
                VALUES (?,    ?,     ?,       ?,            ?,           ?,          ?)
            """, (data_str, novo_caixa, base_caixa2, novo_vendas,
                  novo_caixa_total, novo_caixa2_dia, novo_caixa2_tot))

        # 4) LIVRO: **uma linha por lançamento** (sem agregar no dia)
        trans_uid = str(uuid.uuid4())  # UNIQUE por linha
        observ = (
            f"Transferência p/ Caixa 2 | "
            f"Valor={formatar_valor(valor_f)} | "
            f"C={usar_caixa:.2f}; V={usar_vendas:.2f}"
        )
        cur.execute("""
            INSERT INTO movimentacoes_bancarias
                (data, banco,   tipo,     valor,  origem,               observacao,
                 referencia_id, referencia_tabela, trans_uid)
            VALUES (?,   ?,      ?,        ?,      ?,                    ?,
                    ?,             ?,                 ?)
        """, (
            data_str, "Caixa 2", "entrada", valor_f,
            "transferencia_caixa", observ,
            None, "movimentacoes_bancarias", trans_uid
        ))
        mov_id = cur.lastrowid

        # Self-reference: mesma linha recebe referencia_id = seu id e REF na observação
        observ_final = observ + f" | REF={mov_id}"
        cur.execute("""
            UPDATE movimentacoes_bancarias
               SET referencia_id = ?, observacao = ?
             WHERE id = ?
        """, (mov_id, observ_final, mov_id))

        conn.commit()

    return {
        "ok": True,
        "msg": (
            f"✅ Transferência para Caixa 2 registrada: {formatar_valor(valor_f)} "
            f"(abatido — Caixa: {formatar_valor(usar_caixa)}, Vendas: {formatar_valor(usar_vendas)})."
        ),
        "valor": valor_f,
        "usar_caixa": usar_caixa,
        "usar_vendas": usar_vendas,
    }
