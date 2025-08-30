#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teste de pagamentos (CLI) para BOLETO, EMPRESTIMO e FATURA_CARTAO.

Roda 3 cenários em cada tipo (na ordem das 3 parcelas mais recentes):
  1) Desconto: base 100, desconto 50 -> saida_total=50,  vpa=100, desc+=50, status=QUITADO
  2) Juros+Multa: base 100, juros 10, multa 5 -> saida_total=115, vpa=115, j+=10, m+=5, status=QUITADO
  3) Juros+Desconto: base 100, juros 10, desconto 5 -> saida_total=105, vpa=110, j+=10, d+=5, status=QUITADO

Uso:
  python tests/test_pagamentos_cli.py --db ./data/flowdash_data.db

Obs.:
- O script modifica as 3 parcelas mais recentes de cada tipo (marcando-as como QUITADAS).
- Para "FATURA_CARTAO", a busca considera tipo_obrigacao='FATURA_CARTAO' OU tipo_origem='FATURA_CARTAO'.
"""

import argparse
from datetime import date
from importlib import import_module
import sys

# ---------- Imports do projeto ----------
# shared.db.get_conn (para leituras diretas)
try:
    from shared.db import get_conn
except Exception as e:
    print("ERRO: não consegui importar shared.db.get_conn:", e, file=sys.stderr)
    sys.exit(1)

# Monta um repositório CAP combinando Mixins + BaseRepo (sem depender de nomes exatos)
def build_cap_repo(db_path: str):
    try:
        base_mod = import_module("repository.contas_a_pagar_mov_repository.base")
        events_mod = import_module("repository.contas_a_pagar_mov_repository.events")
        payments_mod = import_module("repository.contas_a_pagar_mov_repository.payments")
    except Exception as e:
        print("ERRO: não consegui importar módulos do CAP repository:", e, file=sys.stderr)
        sys.exit(1)

    BaseRepo = getattr(base_mod, "BaseRepo", None)
    EventsMixin = getattr(events_mod, "EventsMixin", None)
    PaymentsMixin = getattr(payments_mod, "PaymentsMixin", None)
    if not (BaseRepo and EventsMixin and PaymentsMixin):
        print("ERRO: BaseRepo / EventsMixin / PaymentsMixin não encontrados.", file=sys.stderr)
        sys.exit(1)

    class CAPRepo(EventsMixin, PaymentsMixin, BaseRepo):  # type: ignore[misc]
        def __init__(self, db_path: str):
            super().__init__(db_path_like=db_path)
            self.db_path = db_path

    return CAPRepo(db_path)


def get_cols(cur, table: str) -> set:
    try:
        return {r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def fetch_latest_parcelas(conn, tipo: str, n: int = 3):
    cur = conn.cursor()
    if tipo == "FATURA_CARTAO":
        rows = cur.execute(
            """
            SELECT id, obrigacao_id, COALESCE(valor_evento,0) AS base
            FROM contas_a_pagar_mov
            WHERE categoria_evento='LANCAMENTO'
              AND (tipo_obrigacao='FATURA_CARTAO' OR tipo_origem='FATURA_CARTAO')
            ORDER BY id DESC
            LIMIT ?
            """,
            (n,),
        ).fetchall()
    else:
        rows = cur.execute(
            """
            SELECT id, obrigacao_id, COALESCE(valor_evento,0) AS base
            FROM contas_a_pagar_mov
            WHERE categoria_evento='LANCAMENTO' AND tipo_obrigacao=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (tipo, n),
        ).fetchall()
    return [{"id": int(r[0]), "obrigacao_id": int(r[1]), "base": float(r[2])} for r in rows]


def read_parcela_state(conn, parcela_id: int) -> dict:
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM contas_a_pagar_mov WHERE id = ?", (parcela_id,)).fetchone()
    if not row:
        raise RuntimeError(f"Parcela id={parcela_id} não encontrada.")
    cols = [d[0] for d in cur.description]  # names in order
    data = dict(zip(cols, row))

    # Normaliza nomes possíveis
    out = {
        "valor_evento": float(data.get("valor_evento", 0.0) or 0.0),
        "valor_pago_acumulado": float(data.get("valor_pago_acumulado", 0.0) or 0.0)
            if "valor_pago_acumulado" in cols else None,
        "juros_pago": float(data.get("juros_pago", 0.0) or 0.0) if "juros_pago" in cols else 0.0,
        "status": data.get("status", None),
    }
    # multa
    if "multa_pago" in cols:
        out["multa_pago"] = float(data.get("multa_pago", 0.0) or 0.0)
    elif "multa_paga" in cols:
        out["multa_pago"] = float(data.get("multa_paga", 0.0) or 0.0)
    else:
        out["multa_pago"] = None
    # desconto
    if "desconto" in cols:
        out["desconto"] = float(data.get("desconto", 0.0) or 0.0)
    elif "desconto_aplicado" in cols:
        out["desconto"] = float(data.get("desconto_aplicado", 0.0) or 0.0)
    else:
        out["desconto"] = None

    return out


def approx(a: float, b: float, eps: float = 0.01) -> bool:
    return abs(float(a) - float(b)) <= eps


def run_scenarios_for_type(db_path: str, tipo: str) -> int:
    """
    Retorna 0 se todos OK, >0 se houve falhas.
    """
    failures = 0
    cap_repo = build_cap_repo(db_path)
    today = date.today().isoformat()

    with get_conn(db_path) as conn:
        parcelas = fetch_latest_parcelas(conn, tipo, 3)
        if len(parcelas) < 3:
            print(f"[{tipo}] AVISO: encontrei apenas {len(parcelas)} parcelas LANCAMENTO; preciso de 3.")
            return 1

        scenarios = [
            ("DESCONTO", dict(juros=0.0, multa=0.0, desconto=50.0), dict(saida=50.0, vpa=100.0, j=0.0, m=0.0, d=50.0)),
            ("JUROS+MULTA", dict(juros=10.0, multa=5.0, desconto=0.0), dict(saida=115.0, vpa=115.0, j=10.0, m=5.0, d=0.0)),
            ("JUROS+DESCONTO", dict(juros=10.0, multa=0.0, desconto=5.0), dict(saida=105.0, vpa=110.0, j=10.0, m=0.0, d=5.0)),
        ]

        print(f"\n=== Testando {tipo} ===")
        for (label, incs, expected), parc in zip(scenarios, parcelas):
            parcela_id = parc["id"]
            print(f"- Cenário {label}: parcela_id={parcela_id}")

            # Aplica quitação total via repo (ledger_id=0 em teste; forma/origem=AJUSTE/CLI)
            res = cap_repo.aplicar_pagamento_parcela_quitacao_total(
                None,
                parcela_id=int(parcela_id),
                juros=float(incs["juros"]),
                multa=float(incs["multa"]),
                desconto=float(incs["desconto"]),
                data_evento=today,
                forma_pagamento="AJUSTE",
                origem="CLI_TEST",
                ledger_id=0,
                usuario="tester",
            )

            after = read_parcela_state(conn, parcela_id)

            # Verificações:
            ok_status = (after["status"] or "").upper() in ("QUITADO", "QUITADA", "QUITADO ", "Quitado")
            ok_saida = approx(res["saida_total"], expected["saida"])
            ok_vpa = True if after["valor_pago_acumulado"] is None else approx(after["valor_pago_acumulado"], expected["vpa"])
            ok_j = approx(after["juros_pago"], expected["j"])

            # multa/desc podem não existir na schema (None = ignora checagem)
            ok_m = True
            if after["multa_pago"] is not None:
                ok_m = approx(after["multa_pago"], expected["m"])

            ok_d = True
            if after["desconto"] is not None:
                ok_d = approx(after["desconto"], expected["d"])

            if all([ok_status, ok_saida, ok_vpa, ok_j, ok_m, ok_d]):
                print(f"  ✅ OK | saida_total={res['saida_total']:.2f} | vpa={after['valor_pago_acumulado']} | j={after['juros_pago']} | m={after['multa_pago']} | d={after['desconto']} | status={after['status']}")
            else:
                failures += 1
                print("  ❌ FALHA:")
                print(f"     - status QUITADO? {ok_status} (status={after['status']})")
                print(f"     - saida_total ok? {ok_saida} (got={res['saida_total']:.2f} exp={expected['saida']:.2f})")
                print(f"     - valor_pago_acumulado ok? {ok_vpa} (got={after['valor_pago_acumulado']} exp={expected['vpa']})")
                print(f"     - juros_pago ok? {ok_j} (got={after['juros_pago']} exp={expected['j']})")
                print(f"     - multa_pago ok? {ok_m} (got={after['multa_pago']} exp={expected['m']})")
                print(f"     - desconto ok? {ok_d} (got={after['desconto']} exp={expected['d']})")

    return failures


def main():
    parser = argparse.ArgumentParser(description="Teste CLI de pagamentos CAP (Boleto/Emprestimo/Fatura).")
    parser.add_argument("--db", required=True, help="Caminho do banco de dados SQLite (ex.: ./data/flowdash_data.db)")
    args = parser.parse_args()
    db_path = args.db

    overall_failures = 0
    for tipo in ("BOLETO", "EMPRESTIMO", "FATURA_CARTAO"):
        overall_failures += run_scenarios_for_type(db_path, tipo)

    print("\n=== RESUMO ===")
    if overall_failures == 0:
        print("✅ Todos os cenários passaram!")
        sys.exit(0)
    else:
        print(f"❌ {overall_failures} cenário(s) falharam.")
        sys.exit(1)


if __name__ == "__main__":
    main()
