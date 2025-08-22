
"""Fatura de cartão (pagamento) (_FaturaLedgerMixin)."""
from __future__ import annotations
from typing import Optional, Tuple
from shared.db import get_conn
from shared.ids import sanitize

class _FaturaLedgerMixin:
    """Mixin de regras para pagamento de fatura de cartão."""

    def pagar_fatura_cartao(
        self,
        *,
        data: str,
        valor: float,
        forma_pagamento: str,
        origem: str,
        obrigacao_id: int,
        usuario: str,
        categoria: Optional[str] = "Fatura Cartão de Crédito",
        sub_categoria: Optional[str] = None,
        descricao: Optional[str] = None,
        trans_uid: Optional[str] = None,
        multa: float = 0.0,
        juros: float = 0.0,
        desconto: float = 0.0,
    ) -> tuple[int, int, int]:
        v_pg = max(0.0, float(valor))
        v_multa = max(0.0, float(multa or 0.0))
        v_juros = max(0.0, float(juros or 0.0))
        v_desc  = max(0.0, float(desconto or 0.0))

        cat  = sanitize(categoria)
        sub  = sanitize(sub_categoria)
        desc = sanitize(descricao)
        usu  = sanitize(usuario)
        org  = sanitize(origem)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            row = cur.execute("""
                SELECT id, COALESCE(valor_evento,0.0) AS valor_parcela
                  FROM contas_a_pagar_mov
                 WHERE obrigacao_id = ?
                   AND categoria_evento = 'LANCAMENTO'
                   AND (tipo_obrigacao='FATURA_CARTAO' OR tipo_origem='FATURA_CARTAO')
                 LIMIT 1
            """, (int(obrigacao_id),)).fetchone()
            if not row:
                raise ValueError(f"Fatura (obrigacao_id={obrigacao_id}) não encontrada.")
            parcela_id = int(row[0])
            valor_parcela = float(row[1])

            saldo_atual = self.cap_repo.obter_saldo_obrigacao(conn, int(obrigacao_id))
            eps = 0.005
            valor_a_pagar = min(v_pg, max(saldo_atual, 0.0))

            total_saida = max(valor_a_pagar + v_multa + v_juros - v_desc, 0.0)

            resumo_aj = []
            if v_multa > 0: resumo_aj.append(f"multa R$ {v_multa:.2f}")
            if v_juros > 0: resumo_aj.append(f"juros R$ {v_juros:.2f}")
            if v_desc  > 0: resumo_aj.append(f"desconto R$ {v_desc:.2f}")
            obs_extra = (" | " + ", ".join(resumo_aj)) if resumo_aj else ""

            if forma_pagamento == "DINHEIRO":
                self._garantir_linha_saldos_caixas(conn, data)
                cur.execute("""
                    INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                       Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                       Origem_Dinheiro, Banco_Saida)
                    VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
                """, (data, cat, sub, desc, float(total_saida), usu, org))
                id_saida = int(cur.lastrowid)

                campo = "caixa" if org == "Caixa" else "caixa_2"
                cur.execute(f"""
                    UPDATE saldos_caixas SET {campo} = COALESCE({campo},0) - ?
                    WHERE data = ?
                """, (float(total_saida), data))

                obs = (f"Pagamento Fatura • {cat}/{sub or ''}".strip()
                       + (f" - {desc}" if desc else "")
                       + obs_extra)
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                    VALUES (?, ?, 'saida', ?, 'saidas_fatura_pagamento', ?, 'saida', ?, ?)
                """, (data, org, float(total_saida), obs, id_saida, trans_uid))
                id_mov = int(cur.lastrowid)
            else:
                self._ajustar_banco_dynamic(conn, banco_col=org, delta=-float(total_saida), data=data)
                cur.execute("""
                    INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                       Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                       Origem_Dinheiro, Banco_Saida)
                    VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
                """, (data, cat, sub, desc, forma_pagamento, float(total_saida), usu, org))
                id_saida = int(cur.lastrowid)

                obs = (f"Pagamento Fatura • {cat}/{sub or ''}".strip()
                       + (f" - {desc}" if desc else "")
                       + obs_extra)
                cur.execute("""
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                    VALUES (?, ?, 'saida', ?, 'saidas_fatura_pagamento', ?, 'saida', ?, ?)
                """, (data, org, float(total_saida), obs, id_saida, trans_uid))
                id_mov = int(cur.lastrowid)

            if v_pg > saldo_atual + eps:
                cur.execute("""
                    UPDATE movimentacoes_bancarias
                       SET observacao = COALESCE(observacao,'') || ' [valor ajustado ao saldo: R$ ' || printf('%.2f', ?) || ']'
                     WHERE id = ?
                """, (float(total_saida), id_mov))

            evento_id = 0
            if total_saida > eps:
                evento_id = self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id=int(obrigacao_id),
                    tipo_obrigacao="FATURA_CARTAO",
                    valor_pago=float(total_saida),
                    data_evento=data,
                    forma_pagamento=forma_pagamento,
                    origem=org,
                    ledger_id=id_saida,
                    usuario=usu,
                )

            self.cap_repo.aplicar_pagamento_parcela(
                conn,
                parcela_id=parcela_id,
                valor_parcela=float(valor_parcela),
                valor_pago_total=float(total_saida),
                juros=float(v_juros),
                multa=float(v_multa),
                desconto=float(v_desc),
            )

            conn.commit()
            return (id_saida, id_mov, int(evento_id))
