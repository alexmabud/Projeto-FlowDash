
"""
Smoke test: pagar_parcela_emprestimo

- Pega uma EMPRESTIMO em aberto (ou usa --obrigacao).
- Aplica pagamento com (valor, juros, multa, desconto).
- Se total a desembolsar <= 0, não movimenta caixa/banco (apenas aplica desconto/encargos).
- Mostra snapshot antes/depois.
"""

import os, argparse, sqlite3
from datetime import datetime
from typing import Optional, Tuple

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
import sys
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from services.ledger.service_ledger_emprestimo import _EmprestimoLedgerMixin


DB = os.path.join(ROOT, "data", "flowdash_data.db")


def pick_emprestimo_obrigacao(conn, preferida: Optional[int]=None) -> Tuple[int,int,float]:
    cur = conn.cursor()
    if preferida:
        row = cur.execute("""
            SELECT id, COALESCE(valor_evento,0) AS base
              FROM contas_a_pagar_mov
             WHERE obrigacao_id = ?
               AND categoria_evento='LANCAMENTO'
               AND (tipo_obrigacao='EMPRESTIMO' OR tipo_origem='EMPRESTIMO')
             LIMIT 1
        """, (int(preferida),)).fetchone()
        if not row: raise RuntimeError(f"Obrigação {preferida} não encontrada como EMPRESTIMO.")
        return int(preferida), int(row[0]), float(row[1])

    row = conn.execute("""
      SELECT m.obrigacao_id
      FROM (
        SELECT DISTINCT obrigacao_id
        FROM contas_a_pagar_mov
        WHERE categoria_evento='LANCAMENTO'
          AND (tipo_obrigacao='EMPRESTIMO' OR tipo_origem='EMPRESTIMO')
      ) m
      LEFT JOIN vw_cap_saldos v ON v.obrigacao_id = m.obrigacao_id
      WHERE COALESCE(v.saldo_aberto,0) > 0.005
      ORDER BY m.obrigacao_id
      LIMIT 1
    """).fetchone()
    if not row:
        raise RuntimeError("Não encontrei EMPRESTIMO em aberto para testar.")
    obrig = int(row[0])
    p = conn.execute("""
        SELECT id, COALESCE(valor_evento,0)
          FROM contas_a_pagar_mov
         WHERE obrigacao_id = ?
           AND categoria_evento='LANCAMENTO'
         LIMIT 1
    """, (obrig,)).fetchone()
    return obrig, int(p[0]), float(p[1])


def snapshot(conn, parcela_id: int):
    conn.row_factory = sqlite3.Row
    return conn.execute("""
      SELECT id, obrigacao_id, tipo_obrigacao, categoria_evento,
             valor_evento, status, valor_pago_acumulado,
             juros_pago, multa_paga, desconto_aplicado, data_pagamento
        FROM contas_a_pagar_mov WHERE id = ?
    """, (parcela_id,)).fetchone()


class MiniService(_EmprestimoLedgerMixin):
    def __init__(self, db_path):
        self.db_path = db_path

    # Helpers mínimos para caixa/banco usados no mixin
    def _garantir_linha_saldos_caixas(self, conn, data: str):
        conn.execute("""
          INSERT OR IGNORE INTO saldos_caixas(data, caixa, caixa_2)
          VALUES (?, COALESCE((SELECT caixa FROM saldos_caixas WHERE data=?),0),
                     COALESCE((SELECT caixa_2 FROM saldos_caixas WHERE data=?),0))
        """, (data, data, data))

    def _garantir_linha_saldos_bancos(self, conn, data: str):
        # se não existir, cria uma linha vazia
        cols = [r[1] for r in conn.execute("PRAGMA table_info(saldos_bancos);")]
        placeholders = ",".join(["?"]*len(cols))
        if not conn.execute("SELECT 1 FROM saldos_bancos WHERE data = ? LIMIT 1", (data,)).fetchone():
            # monta insert com NULLs
            vals = []
            for c in cols:
                vals.append(data if c=="data" else None)
            conn.execute(f"INSERT INTO saldos_bancos({','.join(cols)}) VALUES ({placeholders})", vals)

    def _ajustar_banco_dynamic(self, conn, *, banco_col: str, delta: float, data: str):
        # Atualiza a coluna do banco (se existir)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(saldos_bancos);")]
        if banco_col not in cols:
            # se não existir, apenas registra um warning leve (não quebra o teste)
            print(f"[warn] Coluna '{banco_col}' não existe em saldos_bancos; pulando ajuste.")
            return
        conn.execute(f"UPDATE saldos_bancos SET {banco_col} = COALESCE({banco_col},0) + ? WHERE data = ?", (float(delta), data))

    # cap_repo mínimo usado pela rotina
    class _CapRepo:
        def obter_saldo_obrigacao(self, conn, obrigacao_id: int) -> float:
            r = conn.execute("SELECT COALESCE(saldo_aberto,0) FROM vw_cap_saldos WHERE obrigacao_id = ?", (int(obrigacao_id),)).fetchone()
            return float(r[0]) if r else 0.0

        def registrar_pagamento(self, conn, *, obrigacao_id: int, tipo_obrigacao: str, valor_pago: float, data_evento: str, forma_pagamento: str, origem: str, ledger_id: Optional[int], usuario: str) -> int:
            conn.execute("""
              INSERT INTO contas_a_pagar_mov
                (obrigacao_id, tipo_obrigacao, categoria_evento, data_evento,
                 valor_evento, forma_pagamento, origem, usuario)
              VALUES (?, ?, 'PAGAMENTO', ?, ?, ?, ?, ?)
            """, (int(obrigacao_id), tipo_obrigacao, data_evento, -abs(float(valor_pago)), forma_pagamento, origem, usuario))
            return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

        def aplicar_pagamento_parcela(self, conn, *, parcela_id: int, valor_parcela: float, valor_pago_total: float, juros: float=0.0, multa: float=0.0, desconto: float=0.0, data_pagamento: Optional[str]=None):
            # lê acumulados atuais
            row = conn.execute("""
              SELECT COALESCE(valor_evento,0),
                     COALESCE(valor_pago_acumulado,0),
                     COALESCE(juros_pago,0),
                     COALESCE(multa_paga,0),
                     COALESCE(desconto_aplicado,0),
                     obrigacao_id
                FROM contas_a_pagar_mov
               WHERE id = ?
            """, (int(parcela_id),)).fetchone()
            if not row: raise RuntimeError("parcela não encontrada")

            base, pago_atual, j_atual, m_atual, d_atual, obrig_id = row
            novo_pago = float(pago_atual) + float(valor_pago_total)
            novo_j    = float(j_atual)   + float(juros)
            novo_m    = float(m_atual)   + float(multa)
            novo_d    = float(d_atual)   + float(desconto)

            conn.execute("""
              UPDATE contas_a_pagar_mov
                 SET valor_pago_acumulado = ?,
                     juros_pago = ?, multa_paga = ?, desconto_aplicado = ?,
                     data_pagamento = COALESCE(?, data_pagamento)
               WHERE id = ?
            """, (novo_pago, novo_j, novo_m, novo_d, data_pagamento, int(parcela_id)))

            # saldo e status
            saldo = float(base) + novo_j + novo_m - novo_d - novo_pago
            if abs(saldo) <= 0.005:
                # quita todos os LANCAMENTOS da obrigação
                conn.execute("""
                  UPDATE contas_a_pagar_mov
                     SET status='Quitado'
                   WHERE obrigacao_id=? AND categoria_evento='LANCAMENTO'
                """, (int(obrig_id),))
            else:
                conn.execute("UPDATE contas_a_pagar_mov SET status='Parcial' WHERE id = ?", (int(parcela_id),))

        # compat com mixin (instância)
    @property
    def cap_repo(self):
        return self._cap
    _cap: _CapRepo = _CapRepo()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--obrigacao", type=int, default=None)
    ap.add_argument("--forma", default="DINHEIRO", choices=["DINHEIRO","PIX","DÉBITO"])
    ap.add_argument("--origem", default="Caixa")
    ap.add_argument("--valor", type=float, default=50.0)
    ap.add_argument("--juros", type=float, default=0.0)
    ap.add_argument("--multa", type=float, default=0.0)
    ap.add_argument("--desconto", type=float, default=50.0)
    ap.add_argument("--usuario", default="tester")
    ap.add_argument("--categoria", default="Empréstimos e Financiamentos")
    ap.add_argument("--sub", default=None)
    ap.add_argument("--desc", default="Teste pagamento emprestimo")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print("==== CONTEXTO ====")
    print("DB:", DB)

    with sqlite3.connect(DB) as conn:
        obrig_id, parcela_id, valor_base = pick_emprestimo_obrigacao(conn, args.obrigacao)
        print(f"obrigacao_id: {obrig_id}  parcela_id: {parcela_id}")
        print(f"valor_evento(BASE): {valor_base:.2f}")
        print(f"pagamento: base={args.valor:.2f}  juros={args.juros:.2f}  multa={args.multa:.2f}  desc={args.desconto:.2f}")
        print(f"forma={args.forma}  origem={args.origem}\n")

        print("Snapshot ANTES:")
        print(snapshot(conn, parcela_id))

        svc = MiniService(DB)
        ids = svc.pagar_parcela_emprestimo(
            data=datetime.now().strftime("%Y-%m-%d"),
            valor=args.valor,
            forma_pagamento=args.forma,
            origem=args.origem,
            obrigacao_id=obrig_id,
            usuario=args.usuario,
            categoria=args.categoria,
            sub_categoria=args.sub,
            descricao=args.desc,
            multa=args.multa,
            juros=args.juros,
            desconto=args.desconto,
        )
        print("\n>>> Chamando pagar_parcela_emprestimo ...")
        print("IDs retornados (id_saida, id_mov, id_evento_cap) =", ids)

        print("\nSnapshot DEPOIS:")
        print(snapshot(conn, parcela_id))

        # mostra saldo aberto
        r = conn.execute("SELECT COALESCE(saldo_aberto,0) FROM vw_cap_saldos WHERE obrigacao_id=?", (int(obrig_id),)).fetchone()
        print(f"\nSaldo aberto após operação: {float(r[0]) if r else 0.0:.2f}")

        if args.dry_run:
            print("\n[DRY-RUN] ROLLBACK feito — nada foi gravado.")
            conn.rollback()
        else:
            print("\n[COMMIT] Alterações persistidas.")
            conn.commit()


if __name__ == "__main__":
    main()

