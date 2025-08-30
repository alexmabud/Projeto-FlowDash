

from __future__ import annotations
import os, sys, argparse, sqlite3
from datetime import date
from pprint import pprint

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from shared.db import get_conn

# Tenta localizar o repo combinado
try:
    from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository as CapRepo  # type: ignore
except Exception:
    try:
        from repository.contas_a_pagar_mov_repository.base import ContasAPagarMovRepository as CapRepo  # type: ignore
    except Exception:
        CapRepo = None  # vamos falhar mais à frente com mensagem clara

from services.ledger.service_ledger_fatura import _FaturaLedgerMixin


class TestLedger(_FaturaLedgerMixin):
    def __init__(self, db_path: str):
        self.db_path = db_path
        if CapRepo is None:
            raise RuntimeError("Não consegui importar ContasAPagarMovRepository.")
        try:
            self.cap_repo = CapRepo(db_path=db_path)
        except Exception:
            self.cap_repo = CapRepo()
            setattr(self.cap_repo, "db_path", db_path)

    def _garantir_linha_saldos_caixas(self, conn: sqlite3.Connection, data: str) -> None:
        conn.execute(
            """
            INSERT INTO saldos_caixas (data, caixa, caixa_2)
            SELECT ?, 0, 0
            WHERE NOT EXISTS (SELECT 1 FROM saldos_caixas WHERE data=?)
            """,
            (data, data),
        )

    def _garantir_linha_saldos_bancos(self, conn: sqlite3.Connection, data: str) -> None:
        conn.execute(
            """
            INSERT INTO saldos_bancos (data)
            SELECT ?
            WHERE NOT EXISTS (SELECT 1 FROM saldos_bancos WHERE data=?)
            """,
            (data, data),
        )

    def _ajustar_banco_dynamic(self, conn: sqlite3.Connection, *, banco_col: str, delta: float, data: str) -> None:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(saldos_bancos)").fetchall()]
        if banco_col not in cols:
            raise ValueError(f"Coluna de banco inválida: {banco_col!r} (existentes: {cols})")
        sql = f"UPDATE saldos_bancos SET {banco_col} = COALESCE({banco_col},0) + ? WHERE data = ?"
        conn.execute(sql, (float(delta), data))


def pick_fatura_obrigacao(conn: sqlite3.Connection, obrigacao_id_cli: int | None):
    if obrigacao_id_cli:
        row = conn.execute(
            """
            SELECT obrigacao_id, id, COALESCE(valor_evento,0.0)
              FROM contas_a_pagar_mov
             WHERE obrigacao_id = ?
               AND categoria_evento='LANCAMENTO'
            LIMIT 1
            """,
            (int(obrigacao_id_cli),),
        ).fetchone()
        if not row:
            raise RuntimeError(f"obrigacao_id={obrigacao_id_cli} não encontrada.")
        return int(row[0]), int(row[1]), float(row[2])

    row = conn.execute(
        """
        SELECT obrigacao_id, id, COALESCE(valor_evento,0.0)
          FROM contas_a_pagar_mov
         WHERE categoria_evento='LANCAMENTO'
           AND (tipo_obrigacao='FATURA_CARTAO' OR tipo_origem='FATURA_CARTAO')
           AND (status IS NULL OR status='' OR status='Em aberto' OR status='Parcial')
         LIMIT 1
        """
    ).fetchone()
    if not row:
        raise RuntimeError("Não encontrei FATURA_CARTAO em aberto para testar.")
    return int(row[0]), int(row[1]), float(row[2])


def get_parcela_snapshot(conn: sqlite3.Connection, parcela_id: int):
    return conn.execute(
        """
        SELECT 
            id, obrigacao_id, valor_evento,
            COALESCE(valor_pago_acumulado,0),
            COALESCE(juros_pago,0),
            COALESCE(multa_paga,0),
            COALESCE(desconto_aplicado,0),
            COALESCE(status,'')
        FROM contas_a_pagar_mov
        WHERE id=?
        """,
        (int(parcela_id),),
    ).fetchone()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/flowdash_data.db")
    ap.add_argument("--obrigacao", type=int, default=None)
    ap.add_argument("--valor", type=float, default=None)
    ap.add_argument("--juros", type=float, default=0.0)
    ap.add_argument("--multa", type=float, default=0.0)
    ap.add_argument("--desconto", type=float, default=0.0)
    ap.add_argument("--forma", default="DINHEIRO", choices=["DINHEIRO", "PIX", "DÉBITO"])
    ap.add_argument("--origem", default="Caixa")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    svc = TestLedger(args.db)

    today = date.today().strftime("%Y-%m-%d")
    with get_conn(args.db) as conn:
        conn.isolation_level = None
        conn.execute("BEGIN")

        obrig_id, parcela_id, valor_base = pick_fatura_obrigacao(conn, args.obrigacao)
        v = args.valor if args.valor is not None else valor_base

        print("==== CONTEXTO ====")
        print(f"DB: {args.db}")
        print(f"obrigacao_id: {obrig_id}  parcela_id: {parcela_id}")
        print(f"valor_evento(BASE): {valor_base:.2f}")
        print(f"pagamento: base={v:.2f}  juros={args.juros:.2f}  multa={args.multa:.2f}  desc={args.desconto:.2f}")
        print(f"forma={args.forma}  origem={args.origem}")
        before = get_parcela_snapshot(conn, parcela_id)
        print("\\nSnapshot ANTES:")
        pprint(before)

        print("\\n>>> Chamando pagar_fatura_cartao ...")
        ids = svc.pagar_fatura_cartao(
            data=today,
            valor=float(v),
            forma_pagamento=args.forma,
            origem=args.origem,
            obrigacao_id=int(obrig_id),
            usuario="SMOKE_TEST",
            categoria="Fatura Cartão de Crédito",
            sub_categoria=None,
            descricao=f"SMOKE TEST {today}",
            trans_uid=None,
            multa=float(args.multa),
            juros=float(args.juros),
            desconto=float(args.desconto),
        )
        print("IDs retornados (id_saida, id_mov, id_evento_cap) =", ids)

        after = get_parcela_snapshot(conn, parcela_id)
        print("\\nSnapshot DEPOIS:")
        pprint(after)

        mov = conn.execute(
            "SELECT id, data, banco, tipo, valor, origem, observacao FROM movimentacoes_bancarias WHERE id=?",
            (ids[1],),
        ).fetchone()
        print("\\nMovimentação bancária criada:")
        pprint(mov)

        if args.dry_run:
            print("\\n[DRY-RUN] ROLLBACK feito — nada foi gravado.")
            conn.execute("ROLLBACK")
        else:
            print("\\n[COMMIT] Alterações persistidas.")
            conn.execute("COMMIT")


if __name__ == "__main__":
    main()

