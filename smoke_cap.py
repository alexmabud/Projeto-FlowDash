# smoke_cap.py
# Smoke tests CAP: valida regra de principal, encargos, desconto e FIFO
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, date
from typing import List, Tuple, Dict

# caminho do banco
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "flowdash_data.db")

# imports do projeto
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository


# --------- helpers de banco ---------
def begin(conn: sqlite3.Connection):
    conn.isolation_level = None
    conn.execute("BEGIN")


def rollback(conn: sqlite3.Connection):
    conn.execute("ROLLBACK")


def fetch_parcela(conn: sqlite3.Connection, parcela_id: int) -> Dict:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    r = cur.execute(
        """
        SELECT id, obrigacao_id, credor, valor_evento,
               COALESCE(principal_pago_acumulado,0)         AS principal_pago_acumulado,
               COALESCE(juros_pago_acumulado,0)             AS juros_pago_acumulado,
               COALESCE(multa_paga_acumulada,0)             AS multa_paga_acumulada,
               COALESCE(desconto_aplicado_acumulado,0)      AS desconto_aplicado_acumulado,
               COALESCE(valor_pago_acumulado,0)             AS valor_pago_acumulado,
               COALESCE(status,'EM ABERTO')                  AS status,
               vencimento
          FROM contas_a_pagar_mov
         WHERE id = ?
        """,
        (parcela_id,),
    ).fetchone()
    return dict(r) if r else {}


def assert_close(a: float, b: float, eps: float = 0.005) -> bool:
    return abs(float(a) - float(b)) <= eps


# --------- criação de parcelas sintéticas ---------
def create_parcelas(
    repo: ContasAPagarMovRepository,
    conn: sqlite3.Connection,
    n: int,
    valor: float,
    *,
    tipo_obrigacao="BOLETO",
    credor="SMOKETEST",
    base_vcto: date = date(2025, 1, 10),
) -> List[int]:
    """Cria n LANCAMENTOS consecutivos (vencimentos mensais) e retorna IDs."""
    ids = []
    # fallback se o repo não tiver proximo_obrigacao_id
    try:
        obrig_base = int(repo.proximo_obrigacao_id(conn))  # type: ignore[attr-defined]
    except Exception:
        cur = conn.cursor()
        row = cur.execute("SELECT COALESCE(MAX(obrigacao_id), 0) FROM contas_a_pagar_mov").fetchone()
        obrig_base = int((row[0] or 0) + 1)

    for i in range(1, n + 1):
        vcto = date(
            base_vcto.year + (base_vcto.month - 1 + (i - 1)) // 12,
            ((base_vcto.month - 1 + (i - 1)) % 12) + 1,
            min(base_vcto.day, 28),
        )
        pid = repo.registrar_lancamento(
            conn,
            obrigacao_id=obrig_base + (i - 1),
            tipo_obrigacao=tipo_obrigacao,
            valor_total=float(valor),
            data_evento=str(vcto),
            vencimento=str(vcto),
            descricao=f"{credor} {i}/{n}",
            credor=credor,
            competencia=str(vcto)[:7],
            parcela_num=i,
            parcelas_total=n,
            usuario="SMOKE",
        )
        ids.append(int(pid))
        conn.execute(
            "UPDATE contas_a_pagar_mov SET status='EM ABERTO', tipo_origem=? WHERE id=?",
            (tipo_obrigacao, int(pid)),
        )
    return ids


# --------- aplicação FIFO manual (encargos só na 1ª) ---------
def aplicar_fifo(
    repo: ContasAPagarMovRepository,
    conn: sqlite3.Connection,
    parcela_ids: List[int],
    total_pago_caixa: float,
    *,
    juros: float = 0.0,
    multa: float = 0.0,
    desconto: float = 0.0,
    data_evt: str = None,
    usuario: str = "SMOKE",
) -> Tuple[float, List[int]]:
    """
    Aplica pagamento FIFO interpretando 'total_pago_caixa' como o que SAIU do caixa/banco.
    Regras:
      - Encargos (juros/multa) são abatidos do caixa na 1ª parcela.
      - O restante do caixa vira orçamento de PRINCIPAL e é cascateado nas parcelas.
      - Desconto NÃO é caixa; aplica-se apenas na 1ª parcela (até o faltante).
    """
    eps = 0.005
    data_evt = data_evt or datetime.now().strftime("%Y-%m-%d")

    # orçamento de principal = caixa - (juros + multa), nunca negativo
    caixa = max(0.0, float(total_pago_caixa))
    j1 = max(0.0, float(juros or 0.0))
    m1 = max(0.0, float(multa or 0.0))
    d1 = max(0.0, float(desconto or 0.0))

    principal_budget = max(0.0, round(caixa - (j1 + m1), 2))

    saida_total = 0.0
    tocadas: List[int] = []

    first = True
    for pid in parcela_ids:
        row = fetch_parcela(conn, pid)
        faltante = max(0.0, float(row.get("valor_evento") or 0.0) - float(row.get("principal_pago_acumulado") or 0.0))
        if faltante <= eps and not first:
            continue

        aplicar_principal = min(principal_budget, faltante)

        if first:
            # desconto só na 1ª, limitado ao faltante após principal
            desc_eff = min(d1, max(0.0, faltante - aplicar_principal))
            snap = repo.aplicar_pagamento_parcela(
                conn,
                parcela_id=int(pid),
                valor_base=float(aplicar_principal),  # principal
                juros=float(j1),
                multa=float(m1),
                desconto=float(desc_eff),
                data_evento=data_evt,
                usuario=usuario,
            )
            first = False
        else:
            snap = repo.aplicar_pagamento_parcela(
                conn,
                parcela_id=int(pid),
                valor_base=float(aplicar_principal),
                juros=0.0,
                multa=0.0,
                desconto=0.0,
                data_evento=data_evt,
                usuario=usuario,
            )

        saida_total = round(saida_total + float(snap.get("saida_total", 0.0)), 2)
        tocadas.append(pid)
        principal_budget = round(principal_budget - aplicar_principal, 2)

        if principal_budget <= eps:
            break

    return saida_total, tocadas


# --------- cenários ---------
def cenario_1(repo: ContasAPagarMovRepository, conn: sqlite3.Connection) -> Tuple[bool, str]:
    # Parcela 100; pago 120 (20 juros) → amortiza 100; gasto 120; QUITADO
    ids = create_parcelas(repo, conn, 1, 100.0, credor="SMK1")
    pid = ids[0]
    today = datetime.now().strftime("%Y-%m-%d")

    snap = repo.aplicar_pagamento_parcela(
        conn,
        parcela_id=int(pid),
        valor_base=100.0,  # principal
        juros=20.0,
        multa=0.0,
        desconto=0.0,
        data_evento=today,
        usuario="SMOKE",
    )
    row = fetch_parcela(conn, pid)
    ok = (
        assert_close(row["principal_pago_acumulado"], 100.0)
        and assert_close(row["valor_pago_acumulado"], 120.0)
        and str(row["status"]).upper() == "QUITADO"
    )
    msg = f"id={pid} principal={row['principal_pago_acumulado']} juros=20.0 caixa={row['valor_pago_acumulado']} status={row['status']} (saida_agregada={snap.get('saida_total',0.0)})"
    return ok, msg


def cenario_2(repo: ContasAPagarMovRepository, conn: sqlite3.Connection) -> Tuple[bool, str]:
    # Parcela 100; desconto 50 + pago 50 → amortiza 100; gasto 50; QUITADO
    ids = create_parcelas(repo, conn, 1, 100.0, credor="SMK2")
    pid = ids[0]
    today = datetime.now().strftime("%Y-%m-%d")

    snap = repo.aplicar_pagamento_parcela(
        conn,
        parcela_id=int(pid),
        valor_base=50.0,   # principal
        juros=0.0,
        multa=0.0,
        desconto=50.0,     # desconto amortiza principal mas não entra no caixa
        data_evento=today,
        usuario="SMOKE",
    )
    row = fetch_parcela(conn, pid)
    ok = (
        assert_close(row["principal_pago_acumulado"], 100.0)
        and assert_close(row["valor_pago_acumulado"], 50.0)
        and str(row["status"]).upper() == "QUITADO"
    )
    msg = f"id={pid} principal={row['principal_pago_acumulado']} desconto=50.0 caixa={row['valor_pago_acumulado']} status={row['status']} (saida_agregada={snap.get('saida_total',0.0)})"
    return ok, msg


def cenario_3(repo: ContasAPagarMovRepository, conn: sqlite3.Connection) -> Tuple[bool, str]:
    # 3×100; pago 150 → P1 quita; P2 parcial 50; gasto 150
    ids = create_parcelas(repo, conn, 3, 100.0, credor="SMK3")
    saida_total, tocadas = aplicar_fifo(repo, conn, ids, total_pago_caixa=150.0)

    r1 = fetch_parcela(conn, ids[0])
    r2 = fetch_parcela(conn, ids[1])

    ok = (
        str(r1["status"]).upper() == "QUITADO"
        and assert_close(r1["principal_pago_acumulado"], 100.0)
        and assert_close(r2["principal_pago_acumulado"], 50.0)
        and assert_close(saida_total, 150.0)
    )
    msg = f"P1(id={ids[0]}) principal=100.0 status={r1['status']}; P2(id={ids[1]}) principal={r2['principal_pago_acumulado']}; caixa_total=150.0 (saida_agregada={saida_total})"
    return ok, msg


def cenario_4(repo: ContasAPagarMovRepository, conn: sqlite3.Connection) -> Tuple[bool, str]:
    # Parcela 100; multa 10; pago 150 → P1 gasta 110, quita; P2 recebe 40 de principal; gasto 150
    ids = create_parcelas(repo, conn, 2, 100.0, credor="SMK4")
    saida_total, tocadas = aplicar_fifo(repo, conn, ids, total_pago_caixa=150.0, multa=10.0)

    r1 = fetch_parcela(conn, ids[0])
    r2 = fetch_parcela(conn, ids[1])

    ok = (
        str(r1["status"]).upper() == "QUITADO"
        and assert_close(r1["valor_pago_acumulado"], 110.0)   # 100 principal + 10 multa
        and assert_close(r2["principal_pago_acumulado"], 40.0)
        and assert_close(saida_total, 150.0)
    )
    msg = f"P1(id={ids[0]}) caixa={r1['valor_pago_acumulado']} status={r1['status']}; P2(id={ids[1]}) principal={r2['principal_pago_acumulado']}; caixa_total=150.0 (saida_agregada={saida_total})"
    return ok, msg


# --------- runner ---------
def main():
    print("== Smoke CAP ==")
    repo = ContasAPagarMovRepository(DB_PATH)
    with sqlite3.connect(DB_PATH) as conn:
        begin(conn)
        try:
            results = []
            for name, fn in [
                ("Cenário 1", cenario_1),
                ("Cenário 2", cenario_2),
                ("Cenário 3", cenario_3),
                ("Cenário 4", cenario_4),
            ]:
                ok, msg = fn(repo, conn)
                print(f"{name}: {'PASS' if ok else 'FAIL'} — {msg}")
                results.append(ok)

            print("\nResumo:", f"{sum(1 for x in results if x)}/{len(results)} PASS")
        finally:
            # não deixa nada na base
            rollback(conn)


if __name__ == "__main__":
    main()
