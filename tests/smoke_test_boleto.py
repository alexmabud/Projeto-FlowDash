#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Smoke test para pagar parcela de BOLETO via _BoletoLedgerMixin.

Uso típico (pagando R$50 com desconto de R$50 — total de saída = 0):
  python .\tests\smoke_test_boleto.py --obrigacao 123 --forma DINHEIRO --origem "Caixa" --valor 50 --desconto 50

Outros exemplos:
  python .\tests\smoke_test_boleto.py --forma DINHEIRO --origem "Caixa" --valor 100
  python .\tests\smoke_test_boleto.py --forma PIX --origem "Nubank" --valor 80 --juros 5
  python .\tests\smoke_test_boleto.py --forma "DÉBITO" --origem "Banco do Brasil" --valor 120 --multa 2

Use --dry-run para não persistir no banco (é criada uma cópia temporária do .db):
  python .\tests\smoke_test_boleto.py --dry-run --valor 100 --forma DINHEIRO --origem "Caixa"
"""

import os
import sys
import shutil
import argparse
import sqlite3
from datetime import date, datetime

# ------------------------------------------------------------------------------------
# Resolve paths e importa o mixin real
# ------------------------------------------------------------------------------------
CUR_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.normpath(os.path.join(CUR_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

DB_DEFAULT = os.path.join(PROJECT_ROOT, "data", "flowdash_data.db")

from services.ledger.service_ledger_boleto import _BoletoLedgerMixin  # noqa: E402


# ------------------------------------------------------------------------------------
# Adaptações mínimas de "service" para o mixin funcionar no teste
# ------------------------------------------------------------------------------------
class MiniService(_BoletoLedgerMixin):
    def __init__(self, db_path: str):
        self.db_path = db_path

        class _CapRepo:
            """Implementa só o que o mixin precisa (métodos mínimos)."""

            @staticmethod
            def obter_saldo_obrigacao(conn, obrigacao_id: int) -> float:
                # saldo = base + juros + multa - desconto - pago_acum (apenas LANCAMENTO)
                row = conn.execute(
                    """
                    SELECT
                        COALESCE(SUM(valor_evento),0)                              AS base,
                        COALESCE(SUM(valor_pago_acumulado),0)                       AS pago,
                        COALESCE(SUM(juros_pago),0)                                 AS juros,
                        COALESCE(SUM(multa_paga),0)                                 AS multa,
                        COALESCE(SUM(desconto_aplicado),0)                          AS desconto
                      FROM contas_a_pagar_mov
                     WHERE obrigacao_id = ?
                       AND categoria_evento = 'LANCAMENTO'
                    """,
                    (int(obrigacao_id),),
                ).fetchone()
                base, pago, juros, multa, desc = [float(x or 0.0) for x in row]
                return round(base + juros + multa - desc - pago, 2)

            @staticmethod
            def registrar_pagamento(
                conn,
                *,
                obrigacao_id: int,
                tipo_obrigacao: str,
                valor_pago: float,
                data_evento: str,
                forma_pagamento: str,
                origem: str,
                ledger_id: int | None,
                usuario: str,
            ) -> int:
                # Evento PAGAMENTO (valor_evento NEGATIVO para registro histórico)
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO contas_a_pagar_mov
                        (obrigacao_id, tipo_obrigacao, categoria_evento, data_evento,
                         valor_evento, descricao, credor, competencia, parcela_num,
                         parcelas_total, forma_pagamento, origem, ledger_id, usuario, status)
                    VALUES (?, ?, 'PAGAMENTO', ?, ?, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, ?, 'Liquidação')
                    """,
                    (
                        int(obrigacao_id),
                        str(tipo_obrigacao),
                        str(data_evento),
                        -abs(float(valor_pago)),
                        str(forma_pagamento),
                        str(origem),
                        int(ledger_id) if ledger_id is not None else None,
                        str(usuario),
                    ),
                )
                return int(cur.lastrowid)

            @staticmethod
            def aplicar_pagamento_parcela(
                conn,
                *,
                parcela_id: int,
                valor_parcela: float,
                valor_pago_total: float,
                juros: float = 0.0,
                multa: float = 0.0,
                desconto: float = 0.0,
                data_pagamento: str | None = None,
            ) -> dict:
                # Atualiza acumulados na linha LANCAMENTO e define status com base no saldo.
                cur = conn.cursor()
                row = cur.execute(
                    """
                    SELECT obrigacao_id,
                           COALESCE(valor_evento,0),
                           COALESCE(valor_pago_acumulado,0),
                           COALESCE(juros_pago,0),
                           COALESCE(multa_paga,0),
                           COALESCE(desconto_aplicado,0)
                      FROM contas_a_pagar_mov
                     WHERE id = ?
                    """,
                    (int(parcela_id),),
                ).fetchone()
                if not row:
                    raise ValueError(f"Parcela id={parcela_id} não encontrada.")

                obrig_id, base, pago_acum, j_acum, m_acum, d_acum = [float(x or 0.0) for x in row]

                novo_pago = round(pago_acum + float(valor_pago_total), 2)
                novo_j = round(j_acum + float(juros), 2)
                novo_m = round(m_acum + float(multa), 2)
                novo_d = round(d_acum + float(desconto), 2)

                cur.execute(
                    """
                    UPDATE contas_a_pagar_mov
                       SET valor_pago_acumulado = ?,
                           juros_pago = ?,
                           multa_paga = ?,
                           desconto_aplicado = ?,
                           data_pagamento = COALESCE(?, data_pagamento)
                     WHERE id = ?
                    """,
                    (novo_pago, novo_j, novo_m, novo_d, data_pagamento, int(parcela_id)),
                )

                saldo = round(base + novo_j + novo_m - novo_d - novo_pago, 2)
                if abs(saldo) <= 0.005:
                    cur.execute(
                        """
                        UPDATE contas_a_pagar_mov
                           SET status='Quitado'
                         WHERE obrigacao_id = ? AND categoria_evento='LANCAMENTO'
                        """,
                        (int(obrig_id),),
                    )
                    status = "Quitado"
                else:
                    cur.execute(
                        "UPDATE contas_a_pagar_mov SET status='Parcial' WHERE id = ?",
                        (int(parcela_id),),
                    )
                    status = "Parcial"

                return {
                    "parcela_id": int(parcela_id),
                    "status": status,
                    "restante": max(0.0, saldo),
                }

        self.cap_repo = _CapRepo()

    # Linhas de suporte usadas pelo mixin
    def _garantir_linha_saldos_caixas(self, conn, data_str: str):
        conn.execute(
            """
            INSERT INTO saldos_caixas (data, caixa, caixa_2)
                 SELECT ?, 0.0, 0.0
              WHERE NOT EXISTS (SELECT 1 FROM saldos_caixas WHERE data = ?)
            """,
            (data_str, data_str),
        )

    def _garantir_linha_saldos_bancos(self, conn, data_str: str):
        # Se houver tabela saldos_bancos, cria linha do dia
        try:
            conn.execute(
                """
                INSERT INTO saldos_bancos (data)
                     SELECT ?
                  WHERE NOT EXISTS (SELECT 1 FROM saldos_bancos WHERE data = ?)
                """,
                (data_str, data_str),
            )
        except Exception:
            pass

    def _ajustar_banco_dynamic(self, conn, *, banco_col: str, delta: float, data: str):
        # Atualiza a coluna do banco informada (deve existir/ser válida no schema)
        try:
            conn.execute(
                f"UPDATE saldos_bancos SET \"{banco_col}\" = COALESCE(\"{banco_col}\",0) + ? WHERE data = ?",
                (float(delta), data),
            )
        except Exception:
            # ignora se não existir tabela/coluna neste ambiente de teste
            pass


# ------------------------------------------------------------------------------------
# Auxiliares
# ------------------------------------------------------------------------------------
def pick_boleto_obrigacao(conn: sqlite3.Connection, preferida: int | None) -> tuple[int, int, float]:
    """
    Escolhe uma BOLETO em aberto e retorna (obrigacao_id, parcela_id, valor_base).
    """
    if preferida:
        row = conn.execute(
            """
            SELECT id, COALESCE(valor_evento,0)
              FROM contas_a_pagar_mov
             WHERE obrigacao_id = ?
               AND categoria_evento='LANCAMENTO'
               AND (tipo_obrigacao='BOLETO' OR tipo_origem='BOLETO')
             LIMIT 1
            """,
            (int(preferida),),
        ).fetchone()
        if not row:
            raise RuntimeError(f"Obrigação {preferida} não encontrada como BOLETO.")
        return int(preferida), int(row[0]), float(row[1])

    row = conn.execute(
        """
        SELECT obrigacao_id, id, COALESCE(valor_evento,0) AS base
          FROM contas_a_pagar_mov
         WHERE categoria_evento='LANCAMENTO'
           AND (tipo_obrigacao='BOLETO' OR tipo_origem='BOLETO')
           AND COALESCE(status,'')!='Quitado'
         ORDER BY obrigacao_id
         LIMIT 1
        """
    ).fetchone()
    if not row:
        raise RuntimeError("Não encontrei BOLETO em aberto para testar.")
    return int(row[0]), int(row[1]), float(row[2])


def snapshot(conn: sqlite3.Connection, parcela_id: int) -> sqlite3.Row:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT id, obrigacao_id, valor_evento, valor_pago_acumulado,
               juros_pago, multa_paga, desconto_aplicado, status, data_pagamento
          FROM contas_a_pagar_mov
         WHERE id = ?
        """,
        (int(parcela_id),),
    ).fetchone()


# ------------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Smoke test pagamento de BOLETO")
    ap.add_argument("--db", default=DB_DEFAULT, help="Caminho do SQLite (default: data/flowdash_data.db)")
    ap.add_argument("--obrigacao", type=int, help="Obrigacao_id específica")
    ap.add_argument("--valor", type=float, default=100.0)
    ap.add_argument("--juros", type=float, default=0.0)
    ap.add_argument("--multa", type=float, default=0.0)
    ap.add_argument("--desconto", type=float, default=0.0)
    ap.add_argument("--forma", required=True, choices=["DINHEIRO", "PIX", "DÉBITO"])
    ap.add_argument("--origem", required=True, help='"Caixa"/"Caixa 2" ou nome do banco')
    ap.add_argument("--usuario", default="Teste")
    ap.add_argument("--descricao", default="Smoke Test Boleto")
    ap.add_argument("--dry-run", action="store_true", help="Executa numa cópia do DB (não persiste)")
    args = ap.parse_args()

    db_path = os.path.abspath(args.db)
    if args.dry_run:
        db_copy = db_path.replace(".db", ".DRYRUN.db")
        shutil.copyfile(db_path, db_copy)
        db_path_use = db_copy
    else:
        db_path_use = db_path

    print("==== CONTEXTO ====")
    print("DB:", db_path_use)

    conn = sqlite3.connect(db_path_use)
    try:
        obrig_id, parcela_id, valor_base = pick_boleto_obrigacao(conn, args.obrigacao)

        print(f"obrigacao_id: {obrig_id}  parcela_id: {parcela_id}")
        print(f"valor_evento(BASE): {valor_base:.2f}")
        print(
            f"pagamento: base={args.valor:.2f}  juros={args.juros:.2f}  multa={args.multa:.2f}  desc={args.desconto:.2f}"
        )
        print(f"forma={args.forma}  origem={args.origem}")

        print("\nSnapshot ANTES:")
        print(snapshot(conn, parcela_id))

        svc = MiniService(db_path=db_path_use)

        print("\n>>> Chamando pagar_parcela_boleto ...")
        ids = svc.pagar_parcela_boleto(
            data=date.today().strftime("%Y-%m-%d"),
            valor=float(args.valor),
            forma_pagamento=args.forma,
            origem=args.origem,
            obrigacao_id=int(obrig_id),
            usuario=args.usuario,
            categoria="Boletos",
            sub_categoria=None,
            descricao=args.descricao,
            trans_uid=None,
            multa=float(args.multa),
            juros=float(args.juros),
            desconto=float(args.desconto),
        )
        print("IDs retornados (id_saida, id_mov, id_evento_cap) =", ids)

        print("\nSnapshot DEPOIS:")
        print(snapshot(conn, parcela_id))

        # Mostra a última movimentação bancária do dia (se houver)
        try:
            mov = conn.execute(
                """
                SELECT id, data, banco, tipo, valor, origem, observacao
                  FROM movimentacoes_bancarias
                 WHERE data = ?
                 ORDER BY id DESC
                 LIMIT 1
                """,
                (date.today().strftime("%Y-%m-%d"),),
            ).fetchone()
            print("\nMovimentação bancária criada:")
            print(mov)
        except Exception:
            print("\nMovimentação bancária criada:")
            print(None)

        if args.dry_run:
            conn.rollback()
            print("\n[DRY-RUN] ROLLBACK feito — nada foi gravado.")
        else:
            conn.commit()
            print("\n[COMMIT] Alterações persistidas.")
    finally:
        conn.close()
        if args.dry_run:
            try:
                os.remove(db_path_use)
            except Exception:
                pass


if __name__ == "__main__":
    main()
