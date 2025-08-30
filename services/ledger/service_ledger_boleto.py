# services/ledger/service_ledger_boleto.py
"""
Boletos (programação e pagamento de parcela).

Regras para programar boletos (gerar N parcelas em `contas_a_pagar_mov`) e
efetuar o pagamento de uma parcela, registrando os efeitos financeiros.

Responsabilidades:
- Programar boletos em N parcelas com competência e vencimentos mensais.
- Registrar o pagamento de parcela (dinheiro ou banco), aplicando juros/multa/desconto.
- Atualizar saldos (caixas ou bancos) e logs em `movimentacoes_bancarias`.
- Integrar com CAP:
    - Lançamento: `registrar_lancamento`
    - Pagamento (QUITACAO TOTAL, sem resíduo): `aplicar_pagamento_parcela_quitacao_total`
      (se valor informado for menor que o saldo, cai para parcial via `aplicar_pagamento_parcela`)

Dependências:
- shared.db.get_conn (controle transacional feito pelo chamador deste mixin).
- shared.ids.sanitize, uid_boleto_programado.
- self.cap_repo, self.mov_repo, self._garantir_linha_saldos_caixas, self._garantir_linha_saldos_bancos,
  self._ajustar_banco_dynamic (expostos pela service/fachada que mistura este mixin).

Notas de segurança:
- SQL apenas com parâmetros (?); sem interpolar dados do usuário.
- Nome de coluna para ajuste de banco é validado via whitelist interna.

Efeitos colaterais:
- Escreve em `contas_a_pagar_mov`, `movimentacoes_bancarias`, `saida`, `saldos_caixas` e (dinâmica) `saldos_bancos`.
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import logging
import os
import sys
from datetime import datetime
from typing import List, Optional, Tuple

# Garante que a raiz do projeto esteja no sys.path (.. .. a partir deste arquivo)
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pandas as pd  # noqa: E402

from shared.db import get_conn  # noqa: E402
from shared.ids import sanitize, uid_boleto_programado  # noqa: E402
from services.ledger.service_ledger_infra import (  # noqa: E402
    _fmt_obs_saida,
    log_mov_bancaria,
)

logger = logging.getLogger(__name__)


class BoletoMixin:
    """Mixin de regras para Boletos (programação e pagamento de parcela)."""

    # ------------------------------------------------------------------
    # Programação de Boletos
    # ------------------------------------------------------------------
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
    ) -> Tuple[List[int], int]:
        """Programa um boleto em N parcelas.

        Cria LANCAMENTOs em CAP (um por parcela) e registra uma linha
        de log em `movimentacoes_bancarias` (origem 'saidas_boleto_programada').

        Returns:
            (ids_mov_cap, id_mov_bancaria). Se `trans_uid` já existir, retorna ([], -1).
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

        # Idempotência: se já existir log com esse trans_uid, não repete
        try:
            if hasattr(self, "mov_repo") and self.mov_repo.ja_existe_transacao(trans_uid):
                logger.info("registrar_saida_boleto: trans_uid já existe (%s) — ignorando", trans_uid)
                return ([], -1)
        except Exception:
            # Em caso de repo indisponível, seguimos (idempotência só via log_mov)
            pass

        compra = pd.to_datetime(data_compra)
        venc1 = pd.to_datetime(vencimento_primeira)

        # Rateio com ajuste na última parcela (para fechar centavos)
        valor_parc = round(float(valor) / int(parcelas), 2)
        ajuste = round(float(valor) - valor_parc * int(parcelas), 2)

        ids_mov_cap: List[int] = []
        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # Obtém base de obrigacao_id (uma sequência por parcela)
            if hasattr(self.cap_repo, "proximo_obrigacao_id"):
                base_obrig_id = int(self.cap_repo.proximo_obrigacao_id(conn))
            else:
                row_max = cur.execute("SELECT COALESCE(MAX(obrigacao_id), 0) FROM contas_a_pagar_mov").fetchone()
                base_obrig_id = int((row_max[0] or 0) + 1)

            for p in range(1, int(parcelas) + 1):
                vcto = (venc1 + pd.DateOffset(months=p - 1)).date()
                vparc = round(valor_parc + (ajuste if p == int(parcelas) else 0.0), 2)

                obrigacao_id = base_obrig_id + (p - 1)

                # Descrição a ir para CAP (inclui Doc se informado)
                desc_base = descricao or f"{fornecedor or 'Fornecedor'} {p}/{int(parcelas)} - {categoria}/{sub_categoria}"
                if documento:
                    desc_base = f"{desc_base} • Doc: {documento}"

                lanc_id = self.cap_repo.registrar_lancamento(
                    conn,
                    obrigacao_id=obrigacao_id,
                    tipo_obrigacao="BOLETO",
                    valor_total=float(vparc),
                    data_evento=str(compra.date()),
                    vencimento=str(vcto),
                    descricao=desc_base,
                    credor=fornecedor,
                    # documento=documento,  # deixe comentado se sua API não aceita
                    competencia=str(vcto)[:7],      # YYYY-MM
                    parcela_num=p,
                    parcelas_total=int(parcelas),
                    usuario=usuario,
                )
                ids_mov_cap.append(int(lanc_id))

            # Marca origem/status para o range de obrigações geradas
            cur.execute(
                """
                UPDATE contas_a_pagar_mov
                   SET tipo_origem='BOLETO',
                       cartao_id=NULL,
                       emprestimo_id=NULL,
                       status = CASE WHEN COALESCE(NULLIF(status,''), '') = '' THEN 'EM ABERTO' ELSE UPPER(status) END
                 WHERE obrigacao_id BETWEEN ? AND ?""",
                (base_obrig_id, base_obrig_id + int(parcelas) - 1),
            )

            # OBS padronizada para o log de "programação de boleto"
            desc_fmt = (descricao or "").strip()
            try:
                p_int = int(parcelas)
                if p_int > 1:
                    desc_fmt = (desc_fmt + f" • {p_int}x").strip()
            except Exception:
                pass

            obs = _fmt_obs_saida(
                forma="BOLETO",
                valor=float(valor),
                categoria=categoria,
                subcategoria=sub_categoria,
                descricao=(desc_fmt or None),
            )

            # Log — inclui usuário e data_hora
            cur.execute(
                """
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao,
                     referencia_tabela, referencia_id, trans_uid, usuario, data_hora)
                VALUES (?, 'Boleto', 'saida', ?, 'saidas_boleto_programada', ?, 'contas_a_pagar_mov', ?, ?, ?, ?)
                """,
                (
                    str(compra.date()),
                    float(valor),
                    obs,
                    ids_mov_cap[0] if ids_mov_cap else None,
                    trans_uid,
                    usuario,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            id_mov = int(cur.lastrowid)

            conn.commit()

        logger.debug(
            "registrar_saida_boleto: parcelas=%s total=%.2f base_obrig=%s ids_cap=%s mov_id=%s",
            parcelas,
            valor,
            base_obrig_id,
            ids_mov_cap,
            id_mov,
        )
        return (ids_mov_cap, id_mov)

    # ------------------------------------------------------------------
    # Pagamento de parcela de Boleto
    # ------------------------------------------------------------------
    def pagar_parcela_boleto(
        self,
        *,
        data: str,
        valor: float,
        forma_pagamento: str,   # "DINHEIRO" | "PIX" | "DÉBITO"
        origem: str,            # "Caixa"/"Caixa 2" ou nome do banco
        obrigacao_id: int,
        usuario: str,
        categoria: Optional[str] = "Boletos",
        sub_categoria: Optional[str] = None,
        descricao: Optional[str] = None,
        trans_uid: Optional[str] = None,
        multa: float = 0.0,
        juros: float = 0.0,
        desconto: float = 0.0,
    ) -> Tuple[int, int, int]:
        """
        Paga uma parcela de boleto ligada a `obrigacao_id`.

        - Se o valor informado for suficiente p/ quitar o principal, usa
          `aplicar_pagamento_parcela_quitacao_total` (sem resíduo).
        - Caso contrário, aplica pagamento **parcial** com `aplicar_pagamento_parcela`
          (principal-only + acumuladores).

        Retorna: (id_saida, id_mov_bancaria, id_evento_cap). Quando idempotente, retorna (-1, -1, -1).
        """
        v_pg    = max(0.0, float(valor))
        v_multa = max(0.0, float(multa or 0.0))
        v_juros = max(0.0, float(juros or 0.0))
        v_desc  = max(0.0, float(desconto or 0.0))

        cat  = sanitize(categoria)
        sub  = sanitize(sub_categoria)
        desc = sanitize(descricao)
        usu  = sanitize(usuario)
        org  = sanitize(origem)

        eps = 0.005

        # Idempotência por trans_uid (se fornecido)
        try:
            if trans_uid and hasattr(self, "mov_repo") and self.mov_repo.ja_existe_transacao(trans_uid):
                logger.info("pagar_parcela_boleto: trans_uid já existe (%s) — ignorando", trans_uid)
                return (-1, -1, -1)
        except Exception:
            pass

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # 1) Ler a parcela-base (principal original) e status
            row = cur.execute(
                """
                SELECT id,
                       COALESCE(valor_evento,0)           AS valor_parcela,
                       COALESCE(valor_pago_acumulado,0)   AS pago_acum,
                       UPPER(COALESCE(status,''))         AS status_atual
                  FROM contas_a_pagar_mov
                 WHERE obrigacao_id = ?
                   AND categoria_evento = 'LANCAMENTO'
                 LIMIT 1
                """,
                (int(obrigacao_id),),
            ).fetchone()
            if not row:
                raise ValueError(f"Parcela (obrigacao_id={obrigacao_id}) não encontrada.")

            parcela_id    = int(row[0])
            valor_parcela = float(row[1])
            pago_acum     = float(row[2])
            status_atual  = (row[3] or "").upper()

            if status_atual == "QUITADA" or (valor_parcela - pago_acum) <= eps:
                logger.info("pagar_parcela_boleto: parcela já quitada (obrigacao_id=%s).", obrigacao_id)
                return (-1, -1, -1)

            # 2) Saldo atual do principal
            saldo_principal = max(valor_parcela - pago_acum, 0.0)

            # 3) Definir cenário: quitação total x parcial
            principal_cliente = max(v_pg - v_desc, 0.0)  # o que o cliente trouxe para principal
            quitacao_total = principal_cliente + eps >= saldo_principal

            principal_efetivo = saldo_principal if quitacao_total else min(principal_cliente, saldo_principal)

            # 4) Valor que sai do caixa/banco
            total_saida = round(principal_efetivo + v_juros + v_multa - v_desc, 2)
            if total_saida < 0:
                total_saida = 0.0

            id_saida = -1
            id_mov   = -1

            # 5) Observação padronizada (usa o valor final que saiu)
            #    Obs.: por padrão não exibimos banco quando forma = PIX no cabeçalho.
            obs = _fmt_obs_saida(
                forma=forma_pagamento,
                valor=float(total_saida),
                categoria=cat,
                subcategoria=sub,
                descricao=desc,
                banco=(org if forma_pagamento == "DÉBITO" else None),
            )

            # 6) Efeito financeiro (saida/saldos) e log — SOMENTE se houver desembolso
            if total_saida > eps:
                data_iso = str(pd.to_datetime(data).date())
                if forma_pagamento == "DINHEIRO":
                    self._garantir_linha_saldos_caixas(conn, data_iso)
                    col_map = {"Caixa": "caixa", "Caixa 2": "caixa_2"}  # whitelist
                    col = col_map.get(org)
                    if not col:
                        raise ValueError(f"Origem de dinheiro inválida para DINHEIRO: {org}")

                    # Saída (DINHEIRO)
                    cur.execute(
                        """
                        INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                           Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                           Origem_Dinheiro, Banco_Saida)
                        VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
                        """,
                        (data_iso, cat, sub, desc, float(total_saida), usu, org),
                    )
                    id_saida = int(cur.lastrowid)

                    # Atualiza saldo do caixa
                    cur.execute(
                        f"UPDATE saldos_caixas SET {col} = COALESCE({col},0) - ? WHERE data = ?",
                        (float(total_saida), data_iso),
                    )

                    # Log financeiro
                    id_mov = log_mov_bancaria(
                        conn,
                        data=data_iso,
                        banco=org,  # "Caixa" / "Caixa 2"
                        tipo="saida",
                        valor=float(total_saida),    # TOTAL (principal + ajustes)
                        origem="saidas_boleto_pagamento",
                        observacao=obs,
                        usuario=usu,
                        referencia_tabela="saida",
                        referencia_id=id_saida,
                        trans_uid=trans_uid,
                        data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )
                else:
                    # Bancos (PIX/DÉBITO)
                    try:
                        self._garantir_linha_saldos_bancos(conn, data_iso)
                    except Exception:
                        pass

                    self._ajustar_banco_dynamic(conn, banco_col=org, delta=-float(total_saida), data=data_iso)

                    # Saída (Banco)
                    cur.execute(
                        """
                        INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                           Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                           Origem_Dinheiro, Banco_Saida)
                        VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
                        """,
                        (data_iso, cat, sub, desc, forma_pagamento, float(total_saida), usu, org),
                    )
                    id_saida = int(cur.lastrowid)

                    # Log financeiro
                    id_mov = log_mov_bancaria(
                        conn,
                        data=data_iso,
                        banco=org,                      # nome do banco
                        tipo="saida",
                        valor=float(total_saida),       # TOTAL (principal + ajustes)
                        origem="saidas_boleto_pagamento",
                        observacao=obs,
                        usuario=usu,
                        referencia_tabela="saida",
                        referencia_id=id_saida,
                        trans_uid=trans_uid,
                        data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                # Observação extra no log se o valor informado diferir do ajustado pelo saldo
                if abs(principal_cliente - principal_efetivo) > eps and id_mov != -1:
                    cur.execute(
                        """
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [valor ajustado ao saldo: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                        """,
                        (float(total_saida), id_mov),
                    )

            # 7) Aplicar no CAP
            if quitacao_total:
                # QUITAÇÃO TOTAL — sem resíduo (status -> QUITADA)
                res = self.cap_repo.aplicar_pagamento_parcela_quitacao_total(
                    conn,
                    parcela_id=int(parcela_id),
                    juros=float(v_juros),
                    multa=float(v_multa),
                    desconto=float(v_desc),
                    data_evento=str(pd.to_datetime(data).date()),
                    forma_pagamento=forma_pagamento,
                    origem=org,
                    ledger_id=(id_saida if id_saida != -1 else 0),
                    usuario=usu,
                )
                try:
                    evento_id = int(res.get("id_evento_cap", -1)) if isinstance(res, dict) else int(res or -1)
                except Exception:
                    evento_id = -1
            else:
                # PARCIAL — principal-only + acumuladores; status pode continuar EM ABERTO
                if hasattr(self.cap_repo, "aplicar_pagamento_parcela"):
                    res = self.cap_repo.aplicar_pagamento_parcela(
                        conn,
                        parcela_id=int(parcela_id),
                        principal=float(principal_efetivo),
                        juros=float(v_juros),
                        multa=float(v_multa),
                        desconto=float(v_desc),
                        data_evento=str(pd.to_datetime(data).date()),
                        usuario=usu,
                    )
                    try:
                        evento_id = int(res.get("id_evento_cap", -1)) if isinstance(res, dict) else int(res or -1)
                    except Exception:
                        evento_id = -1
                else:
                    # Fallback SQL (compatibilidade) — principal-only + acumuladores
                    cur2 = conn.cursor()
                    cur2.execute("PRAGMA table_info('contas_a_pagar_mov')")
                    cols = [r[1] for r in cur2.fetchall()]
                    multa_col = "multa_paga" if "multa_paga" in cols else ("multa_pago" if "multa_pago" in cols else None)
                    desc_col  = "desconto_aplicado" if "desconto_aplicado" in cols else ("desconto" if "desconto" in cols else None)

                    sets = [
                        "valor_pago_acumulado = COALESCE(valor_pago_acumulado,0) + :principal",
                        "juros_pago = COALESCE(juros_pago,0) + :juros",
                        "data_pagamento = DATE(:data_evento)",
                        "status = CASE WHEN COALESCE(valor_evento,0) <= COALESCE(valor_pago_acumulado,0) + :principal THEN 'QUITADA' ELSE 'EM ABERTO' END",
                    ]
                    params = {
                        "principal": float(principal_efetivo),
                        "juros": float(v_juros),
                        "data_evento": str(pd.to_datetime(data).date()),
                        "parcela_id": int(parcela_id),
                    }
                    if multa_col:
                        sets.append(f"{multa_col} = COALESCE({multa_col},0) + :multa")
                        params["multa"] = float(v_multa)
                    if desc_col:
                        sets.append(f"{desc_col} = COALESCE({desc_col},0) + :desconto")
                        params["desconto"] = float(v_desc)

                    cur2.execute(
                        f"UPDATE contas_a_pagar_mov SET {', '.join(sets)} WHERE id = :parcela_id",
                        params,
                    )
                    evento_id = -1

            conn.commit()

        logger.debug(
            "pagar_parcela_boleto: obrig=%s total_saida=%.2f principal_efetivo=%.2f forma=%s origem=%s saida=%s mov=%s evento=%s quitacao_total=%s",
            obrigacao_id,
            total_saida,
            principal_efetivo,
            forma_pagamento,
            org,
            id_saida,
            id_mov,
            evento_id,
            quitacao_total,
        )
        return (id_saida, id_mov, evento_id)


# --- Compatibilidade retroativa: alguns módulos importam _BoletoLedgerMixin ---
_BoletoLedgerMixin = BoletoMixin
__all__ = ["BoletoMixin", "_BoletoLedgerMixin"]
