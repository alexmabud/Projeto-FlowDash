"""
service_ledger_boleto.py — Boletos (programação e pagamento de parcela).

Resumo:
    Regras para programar boletos (gerar N parcelas em contas_a_pagar_mov)
    e efetuar o pagamento de uma parcela, registrando os efeitos financeiros.

Responsabilidades:
    - Programar boletos em N parcelas com competência e vencimentos mensais.
    - Registrar o pagamento de parcela (dinheiro ou banco), aplicando juros/multa/desconto.
    - Atualizar saldos (caixas ou bancos) e logs em movimentacoes_bancarias.
    - Integrar com CAP (registrar_lancamento / registrar_pagamento / aplicar_pagamento_parcela).

Depende de:
    - shared.db.get_conn     (controle transacional feito pelo chamador deste mixin)
    - shared.ids.sanitize, uid_boleto_programado
    - self.cap_repo, self.mov_repo, self._garantir_linha_saldos_caixas, self._ajustar_banco_dynamic
      (expostos pela service/fachada que mistura este mixin)

Notas de segurança:
    - SQL apenas com parâmetros (?); sem interpolar dados do usuário.
    - Nome de coluna para ajuste de caixa é validado via whitelist interna.

Efeitos colaterais:
    - Escreve em contas_a_pagar_mov, movimentacoes_bancarias, saida, saldos_caixas.
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Imports e bootstrap de caminho (torna robusto ao executar pelo Streamlit)
# -----------------------------------------------------------------------------
import logging
from typing import Optional

import os
import sys

# Garante que a raiz do projeto (duas pastas acima de services/ledger) esteja no sys.path
# Estrutura esperada: <RAIZ>/services/ledger/service_ledger_boleto.py
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Dependências de terceiros
import pandas as pd  # noqa: E402

# Módulos internos (absolutos a partir da raiz do projeto)
try:
    from shared.db import get_conn  # noqa: E402
    from shared.ids import sanitize, uid_boleto_programado  # noqa: E402
except Exception as e:
    raise ImportError(
        "Falha ao importar módulos internos. Verifique se a estrutura do projeto contém a pasta "
        "'shared' com __init__.py e arquivos 'db.py' e 'ids.py' na RAIZ do projeto. "
        f"Detalhe do erro original: {e}"
    ) from e

logger = logging.getLogger(__name__)

__all__ = ["_BoletoLedgerMixin", "BoletoMixin"]


class _BoletoLedgerMixin:
    """Mixin de regras para boletos (programar e pagar parcela)."""

    # ------------------------------------------------------------------------------
    # Programação de Boletos
    # ------------------------------------------------------------------------------
    def registrar_saida_boleto(
        self,
        *,
        data_compra: str,
        valor: float,
        parcelas: int,
        vencimento_primeira: str,
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        fornecedor: Optional[str],
        documento: Optional[str],
        trans_uid: Optional[str] = None,
    ) -> tuple[list[int], int]:
        """
        Programa um boleto em N parcelas, gerando LANCAMENTOS em CAP e
        uma linha de log em movimentacoes_bancarias (origem 'saidas_boleto_programada').

        Retorna:
            (ids_mov_cap, id_mov_bancaria)
        """
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if int(parcelas) < 1:
            raise ValueError("Quantidade de parcelas inválida.")

        categoria = sanitize(categoria)
        sub_categoria = sanitize(sub_categoria)
        descricao = sanitize(descricao)
        usuario = sanitize(usuario)
        fornecedor = sanitize(fornecedor)
        documento = sanitize(documento)

        trans_uid = trans_uid or uid_boleto_programado(
            data_compra,
            valor,
            parcelas,
            vencimento_primeira,
            categoria,
            sub_categoria,
            descricao,
            usuario,
        )
        if self.mov_repo.ja_existe_transacao(trans_uid):
            logger.info("registrar_saida_boleto: trans_uid já existe (%s) — ignorando", trans_uid)
            return ([], -1)

        compra = pd.to_datetime(data_compra)
        venc1 = pd.to_datetime(vencimento_primeira)

        # Rateio com ajuste na última parcela (para não sobrar centavos)
        valor_parc = round(float(valor) / int(parcelas), 2)
        ajuste = round(float(valor) - valor_parc * int(parcelas), 2)

        ids_mov_cap: list[int] = []
        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            base_obrig_id = self.cap_repo.proximo_obrigacao_id(conn)

            for p in range(1, int(parcelas) + 1):
                vcto = (venc1 + pd.DateOffset(months=p - 1)).date()
                vparc = round(valor_parc + (ajuste if p == int(parcelas) else 0.0), 2)

                obrigacao_id = base_obrig_id + (p - 1)
                lanc_id = self.cap_repo.registrar_lancamento(
                    conn,
                    obrigacao_id=obrigacao_id,
                    tipo_obrigacao="BOLETO",
                    valor_total=float(vparc),
                    data_evento=str(compra.date()),
                    vencimento=str(vcto),
                    descricao=descricao
                    or f"{fornecedor or 'Fornecedor'} {p}/{int(parcelas)} - {categoria}/{sub_categoria}",
                    credor=fornecedor,
                    competencia=str(vcto)[:7],  # YYYY-MM
                    parcela_num=p,
                    parcelas_total=int(parcelas),
                    usuario=usuario,
                )
                ids_mov_cap.append(int(lanc_id))

            # marca origem e status para o range de obrigacoes geradas
            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET tipo_origem='BOLETO',
                       cartao_id=NULL,
                       emprestimo_id=NULL,
                       status = COALESCE(NULLIF(status,''), 'Em aberto')
                 WHERE obrigacao_id BETWEEN ? AND ?"""
                ,
                (base_obrig_id, base_obrig_id + int(parcelas) - 1),
            )

            obs = f"Boleto {parcelas}x - {categoria}/{sub_categoria}" + (f" - {descricao}" if descricao else "")
            cur.execute(
                """
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                VALUES (?, 'Boleto', 'saida', ?, 'saidas_boleto_programada', ?, 'contas_a_pagar_mov', ?, ?)""",
                (str(compra.date()), float(valor), obs, ids_mov_cap[0] if ids_mov_cap else None, trans_uid),
            )
            id_mov = int(cur.lastrowid)

            conn.commit()

        logger.debug(
            "registrar_saida_boleto: parcelas=%s total=%.2f base_obrig=%s ids_cap=%s mov_id=%s",
            parcelas, valor, base_obrig_id, ids_mov_cap, id_mov
        )
        return (ids_mov_cap, id_mov)

    # ------------------------------------------------------------------------------
    # Pagamento de parcela de Boleto
    # ------------------------------------------------------------------------------
    def pagar_parcela_boleto(
        self,
        *,
        data: str,
        valor: float,
        forma_pagamento: str,
        origem: str,
        obrigacao_id: int,
        usuario: str,
        categoria: Optional[str] = "Boletos",
        sub_categoria: Optional[str] = None,
        descricao: Optional[str] = None,
        trans_uid: Optional[str] = None,
        descricao_extra_cap: Optional[str] = None,  # (reservado p/ integração futura)
        multa: float = 0.0,
        juros: float = 0.0,
        desconto: float = 0.0,
    ) -> tuple[int, int, int]:
        """
        Paga uma parcela de boleto ligada a `obrigacao_id`.

        Efeitos:
            - Registra saída (dinheiro ou banco) com total (valor + multa + juros - desconto).
            - Atualiza saldos de caixa/banco e movimentacoes_bancarias.
            - Registra pagamento em CAP e aplica detalhamento (juros/multa/desconto).

        Retorna:
            (id_saida, id_mov_bancaria, id_evento_cap)
        """
        v_pg = max(0.0, float(valor))
        v_multa = max(0.0, float(multa or 0.0))
        v_juros = max(0.0, float(juros or 0.0))
        v_desc = max(0.0, float(desconto or 0.0))

        cat = sanitize(categoria)
        sub = sanitize(sub_categoria)
        desc = sanitize(descricao)
        usu = sanitize(usuario)
        org = sanitize(origem)

        # Epsilon p/ evitar falso zero
        eps = 0.005

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            saldo_atual = self.cap_repo.obter_saldo_obrigacao(conn, int(obrigacao_id))
            valor_a_pagar = min(v_pg, max(saldo_atual, 0.0))
            total_saida = max(round(valor_a_pagar + v_multa + v_juros - v_desc, 2), 0.0)

            resumo_aj = []
            if v_multa > 0:
                resumo_aj.append(f"multa R$ {v_multa:.2f}")
            if v_juros > 0:
                resumo_aj.append(f"juros R$ {v_juros:.2f}")
            if v_desc > 0:
                resumo_aj.append(f"desconto R$ {v_desc:.2f}")
            obs_extra = (" | " + ", ".join(resumo_aj)) if resumo_aj else ""

            # --- saída e ajuste de saldos
            if forma_pagamento == "DINHEIRO":
                # Atualiza o saldo do caixa correto com whitelist de coluna
                self._garantir_linha_saldos_caixas(conn, data)
                # map de colunas seguras
                col_map = {"Caixa": "caixa", "Caixa 2": "caixa_2"}
                col = col_map.get(org)
                if not col:
                    raise ValueError(f"Origem de dinheiro inválida para DINHEIRO: {org}")

                cur.execute(
                    """
                    INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                       Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                       Origem_Dinheiro, Banco_Saida)
                    VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')""",
                    (data, cat, sub, desc, total_saida, usu, org),
                )
                id_saida = int(cur.lastrowid)

                # Atualiza a coluna validada
                cur.execute(
                    f"UPDATE saldos_caixas SET {col} = COALESCE({col},0) - ? WHERE data = ?",
                    (total_saida, data),
                )

                obs = (f"Pagamento Boleto {cat}/{sub or ''}".strip() + (f" - {desc}" if desc else "") + obs_extra)
                cur.execute(
                    """
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                    VALUES (?, ?, 'saida', ?, 'saidas_boleto_pagamento', ?, 'saida', ?, ?)""",
                    (data, org, total_saida, obs, id_saida, trans_uid),
                )
                id_mov = int(cur.lastrowid)

            else:
                # Debita do banco (coluna dinâmica validada dentro de _ajustar_banco_dynamic)
                self._ajustar_banco_dynamic(conn, banco_col=org, delta=-total_saida, data=data)
                cur.execute(
                    """
                    INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                       Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                       Origem_Dinheiro, Banco_Saida)
                    VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)""",
                    (data, cat, sub, desc, forma_pagamento, total_saida, usu, org),
                )
                id_saida = int(cur.lastrowid)

                obs = (f"Pagamento Boleto {cat}/{sub or ''}".strip() + (f" - {desc}" if desc else "") + obs_extra)
                cur.execute(
                    """
                    INSERT INTO movimentacoes_bancarias
                        (data, banco, tipo, valor, origem, observacao, referencia_tabela, referencia_id, trans_uid)
                    VALUES (?, ?, 'saida', ?, 'saidas_boleto_pagamento', ?, 'saida', ?, ?)""",
                    (data, org, total_saida, obs, id_saida, trans_uid),
                )
                id_mov = int(cur.lastrowid)

            # --- CAP: quitação da parcela + detalhamento (juros/multa/desconto)
            evento_id = 0
            if total_saida > eps:
                evento_id = self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id=int(obrigacao_id),
                    tipo_obrigacao="BOLETO",
                    valor_pago=float(total_saida),
                    data_evento=data,
                    forma_pagamento=forma_pagamento,
                    origem=org,
                    ledger_id=id_saida,
                    usuario=usu,
                )

            row = conn.execute(
                """
                SELECT id, COALESCE(valor_evento,0) AS valor_parcela
                  FROM contas_a_pagar_mov
                 WHERE obrigacao_id = ?
                   AND categoria_evento = 'LANCAMENTO'
                 LIMIT 1""",
                (int(obrigacao_id),),
            ).fetchone()
            if not row:
                raise ValueError(f"Parcela (obrigacao_id={obrigacao_id}) não encontrada.")

            parcela_id = int(row[0])
            valor_parcela = float(row[1])

            self.cap_repo.aplicar_pagamento_parcela(
                conn,
                parcela_id=parcela_id,
                valor_parcela=valor_parcela,
                valor_pago_total=float(total_saida),
                juros=float(v_juros),
                multa=float(v_multa),
                desconto=float(v_desc),
            )

            conn.commit()

        logger.debug(
            "pagar_parcela_boleto: obrig=%s total=%.2f forma=%s origem=%s saida=%s mov=%s evento=%s",
            obrigacao_id, total_saida, forma_pagamento, org, id_saida, id_mov, evento_id
        )
        return (id_saida, id_mov, int(evento_id))
