
"""Saídas (dinheiro/bancária) (_SaidasLedgerMixin).

Implementa:
- registrar_saida_dinheiro
- registrar_saida_bancaria
"""
from __future__ import annotations
import sqlite3
from typing import Optional, Tuple, Dict
from shared.db import get_conn
from shared.ids import sanitize, uid_saida_dinheiro, uid_saida_bancaria

class _SaidasLedgerMixin:
    """Mixin com regras de saída (dinheiro e bancária)."""

    def registrar_saida_dinheiro(
        self,
        *,
        data: str,
        valor: float,
        origem_dinheiro: str,
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        trans_uid: Optional[str] = None,
        vinculo_pagamento: Optional[Dict] = None,
        pagamento_tipo: Optional[str] = None,
        pagamento_destino: Optional[str] = None,
        competencia_pagamento: Optional[str] = None,
        obrigacao_id_fatura: Optional[int] = None,
    ) -> Tuple[int, int]:
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if origem_dinheiro not in ("Caixa", "Caixa 2"):
            raise ValueError("Origem do dinheiro invalida (use 'Caixa' ou 'Caixa 2').")

        categoria     = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao     = sanitize(descricao)
        usuario       = sanitize(usuario)

        trans_uid = trans_uid or uid_saida_dinheiro(
            data, valor, origem_dinheiro, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return (-1, -1)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()
            self._garantir_linha_saldos_caixas(conn, data)

            # (1) INSERT saida
            cur.execute("""
                INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                   Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                   Origem_Dinheiro, Banco_Saida)
                VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
            """, (data, categoria, sub_categoria, descricao, float(valor), usuario, origem_dinheiro))
            id_saida = int(cur.lastrowid)

            # (2) Ajusta saldos de caixa
            campo = "caixa" if origem_dinheiro == "Caixa" else "caixa_2"
            cur.execute(f"""
                UPDATE saldos_caixas SET {campo} = COALESCE({campo},0) - ?
                WHERE data = ?
            """, (float(valor), data))

            # (3) Log
            obs = f"Saida {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'saida', ?, 'saidas', ?, 'saida', ?, ?)
            """, (data, origem_dinheiro, float(valor), obs, id_saida, trans_uid))
            id_mov = int(cur.lastrowid)

            # --- PRIORITÁRIO: pagamento direto pela fatura escolhida na UI ---
            if obrigacao_id_fatura:
                sobra = self._pagar_fatura_por_obrigacao(
                    conn,
                    obrigacao_id=int(obrigacao_id_fatura),
                    valor=float(valor),
                    data_evento=data,
                    forma_pagamento="DINHEIRO",
                    origem=origem_dinheiro,
                    ledger_id=id_saida,
                    usuario=usuario
                )
                if sobra > 0:
                    cur.execute("""
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                    """, (float(sobra), id_mov))
                conn.commit()
                return (id_saida, id_mov)

            # (4) Vinculo direto com um titulo (opcional)
            if vinculo_pagamento:
                obrig_id = int(vinculo_pagamento["obrigacao_id"])
                tipo_obrig = str(vinculo_pagamento["tipo_obrigacao"])
                val = float(vinculo_pagamento.get("valor_pagar", vinculo_pagamento.get("valor_pago", valor)))

                self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id = obrig_id,
                    tipo_obrigacao = tipo_obrig,
                    valor_pago = val,
                    data_evento = data,
                    forma_pagamento = "DINHEIRO",
                    origem = origem_dinheiro,
                    ledger_id = id_saida,
                    usuario = usuario
                )
                self._atualizar_status_por_obrigacao(conn, obrig_id)

            # (5) Classificacao + Auto-baixa por destino/tipo (Fatura/Boletos/Empréstimos)
            if pagamento_tipo and pagamento_destino:
                self._classificar_conta_a_pagar_por_destino(conn, pagamento_tipo, pagamento_destino)
                restante = self._auto_baixar_pagamentos(
                    conn,
                    pagamento_tipo=pagamento_tipo,
                    pagamento_destino=pagamento_destino,
                    valor_total=float(valor),
                    data_evento=data,
                    forma_pagamento="DINHEIRO",
                    origem=origem_dinheiro,
                    ledger_id=id_saida,
                    usuario=usuario,
                    competencia_pagamento=competencia_pagamento
                )
                if restante > 0:
                    cur.execute("""
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                    """, (float(restante), id_mov))

            conn.commit()
            return (id_saida, id_mov)

    def registrar_saida_bancaria(
        self,
        *,
        data: str,
        valor: float,
        banco_nome: str,
        forma: str,               # "PIX" ou "DÉBITO"
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        trans_uid: Optional[str] = None,
        vinculo_pagamento: Optional[Dict] = None,
        pagamento_tipo: Optional[str] = None,
        pagamento_destino: Optional[str] = None,
        competencia_pagamento: Optional[str] = None,
        obrigacao_id_fatura: Optional[int] = None,
    ) -> Tuple[int, int]:
        forma_u = sanitize(forma).upper()
        if forma_u == "DEBITO":
            forma_u = "DÉBITO"
        if forma_u not in ("PIX", "DÉBITO"):
            raise ValueError("Forma invalida para saida bancaria.")
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")

        banco_nome    = sanitize(banco_nome)
        categoria     = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao     = sanitize(descricao)
        usuario       = sanitize(usuario)

        trans_uid = trans_uid or uid_saida_bancaria(
            data, valor, banco_nome, forma_u, categoria, sub_categoria, descricao, usuario
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            return (-1, -1)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # (1) INSERT saida
            cur.execute("""
                INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                   Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                   Origem_Dinheiro, Banco_Saida)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
            """, (data, categoria, sub_categoria, descricao, forma_u, float(valor), usuario, banco_nome))
            id_saida = int(cur.lastrowid)

            # (2) Ajusta saldos de bancos
            self._garantir_linha_saldos_bancos(conn, data)
            self._ajustar_banco_dynamic(conn, banco_nome, -float(valor), data)

            # (3) Log
            obs = f"Saida {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute("""
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, ?, 'saida', ?, 'saidas', ?, 'saida', ?, ?)
            """, (data, banco_nome, float(valor), obs, id_saida, trans_uid))
            id_mov = int(cur.lastrowid)

            # --- PRIORITÁRIO: pagamento direto pela fatura escolhida na UI ---
            if obrigacao_id_fatura:
                sobra = self._pagar_fatura_por_obrigacao(
                    conn,
                    obrigacao_id=int(obrigacao_id_fatura),
                    valor=float(valor),
                    data_evento=data,
                    forma_pagamento=forma_u,
                    origem=banco_nome,
                    ledger_id=id_saida,
                    usuario=usuario
                )
                if sobra > 0:
                    cur.execute("""
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                    """, (float(sobra), id_mov))
                conn.commit()
                return (id_saida, id_mov)

            # (4) Vinculo direto com um titulo (opcional)
            if vinculo_pagamento:
                obrig_id = int(vinculo_pagamento["obrigacao_id"])
                tipo_obrig = str(vinculo_pagamento["tipo_obrigacao"])
                val = float(vinculo_pagamento.get("valor_pagar", vinculo_pagamento.get("valor_pago", valor)))

                self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id = obrig_id,
                    tipo_obrigacao = tipo_obrig,
                    valor_pago = val,
                    data_evento = data,
                    forma_pagamento = forma_u,
                    origem = banco_nome,
                    ledger_id = id_saida,
                    usuario = usuario
                )
                self._atualizar_status_por_obrigacao(conn, obrig_id)

            # (5) Classificacao + Auto-baixa por destino/tipo (Fatura/Boletos/Empréstimos)
            if pagamento_tipo and pagamento_destino:
                self._classificar_conta_a_pagar_por_destino(conn, pagamento_tipo, pagamento_destino)
                restante = self._auto_baixar_pagamentos(
                    conn,
                    pagamento_tipo=pagamento_tipo,
                    pagamento_destino=pagamento_destino,
                    valor_total=float(valor),
                    data_evento=data,
                    forma_pagamento=forma_u,
                    origem=banco_nome,
                    ledger_id=id_saida,
                    usuario=usuario,
                    competencia_pagamento=competencia_pagamento
                )
                if restante > 0:
                    cur.execute("""
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                    """, (float(restante), id_mov))

            conn.commit()
            return (id_saida, id_mov)
