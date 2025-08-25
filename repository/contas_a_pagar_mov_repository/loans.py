"""
Módulo Empréstimos (Contas a Pagar - Mixins)
============================================

Este módulo define a classe `LoansMixin`, responsável por gerar parcelas de
empréstimos e financiamentos na tabela `contas_a_pagar_mov`.

Funcionalidades principais
--------------------------
- Gerar parcelas de empréstimos a partir da tabela `emprestimos_financiamentos`.
- Calcular vencimento de cada parcela considerando `data_inicio_pagamento`,
  `data_contratacao` e `vencimento_dia`.
- Registrar eventos de **LANCAMENTO** (tipo_obrigacao='EMPRESTIMO').
- Marcar parcelas já pagas como quitadas (aplicando pagamento direto).
- Forçar status "Em aberto" para as parcelas restantes.
- Vincular origem (`tipo_origem='EMPRESTIMO'`, `emprestimo_id`).

Detalhes técnicos
-----------------
- Helpers internos:
  - `_label_emprestimo`: define o credor preferindo banco > descrição > tipo.
  - `_add_months`: adiciona meses a uma data, respeitando último dia do mês.
- ESTE MIXIN **NÃO** herda de `BaseRepo`. Ele é combinado com `BaseRepo` na
  classe final (`ContasAPagarMovRepository`), que fornece utilidades como
  `proximo_obrigacao_id` e, via `EventsMixin`, `registrar_lancamento`.

Dependências
------------
- calendar
- datetime (date, datetime)
"""

import calendar
from datetime import date, datetime


class LoansMixin(object):
    """Mixin para geração de parcelas de empréstimos e helpers relacionados."""

    def __init__(self, *args, **kwargs):
        # __init__ cooperativo para múltipla herança
        super().__init__(*args, **kwargs)

    def _label_emprestimo(self, row) -> str:
        """
        Define o campo `credor` que aparecerá em `contas_a_pagar_mov`.

        Prioridade:
            1. banco
            2. descricao
            3. tipo
        """
        for k in ("banco", "descricao", "tipo"):
            v = (row.get(k) or "").strip()
            if v:
                return v
        return "Empréstimo"

    def _add_months(self, d: date, months: int) -> date:
        """
        Soma meses a uma data, ajustando para o último dia do mês quando necessário.
        """
        y = d.year + (d.month - 1 + months) // 12
        m = (d.month - 1 + months) % 12 + 1
        last = calendar.monthrange(y, m)[1]
        day = min(d.day, last)
        return date(y, m, day)

    def gerar_parcelas_emprestimo(
        self,
        conn,
        *,
        emprestimo_id: int,
        usuario: str,
    ) -> dict:
        """
        Cria LANCAMENTOS (tipo_obrigacao='EMPRESTIMO') para todas as parcelas do empréstimo.

        Regras:
            - Para as primeiras `parcelas_pagas`, aplica pagamento direto (status=Quitado).
            - Para as demais, força `status='Em aberto'`.
            - Vincula `tipo_origem='EMPRESTIMO'` e `emprestimo_id`.
            - Não movimenta caixa.

        Retorno
        -------
        dict
            - criadas (int): quantidade de parcelas criadas
            - ajustes_quitadas (int): parcelas marcadas como quitadas
            - obrigacoes (list[int]): lista de obrigacao_id gerados
        """
        # Carrega dados do empréstimo
        row = conn.execute(
            """
            SELECT
                id, banco, descricao, tipo,
                COALESCE(parcelas_total, 0) AS parcelas_total,
                COALESCE(parcelas_pagas, 0) AS parcelas_pagas,
                COALESCE(valor_parcela, 0)  AS valor_parcela,
                data_inicio_pagamento,
                data_contratacao,
                COALESCE(vencimento_dia, 0) AS vencimento_dia
            FROM emprestimos_financiamentos
            WHERE id = ?
            LIMIT 1
            """,
            (int(emprestimo_id),),
        ).fetchone()
        if not row:
            raise ValueError(f"Empréstimo id={emprestimo_id} não encontrado.")

        colunas = [
            "id", "banco", "descricao", "tipo",
            "parcelas_total", "parcelas_pagas", "valor_parcela",
            "data_inicio_pagamento", "data_contratacao", "vencimento_dia",
        ]
        d = dict(zip(colunas, row))

        total_parc = int(d.get("parcelas_total") or 0)
        ja_pagas = max(0, min(total_parc, int(d.get("parcelas_pagas") or 0)))
        vparc = float(d.get("valor_parcela") or 0.0)
        credor = self._label_emprestimo(d)

        if total_parc <= 0 or vparc <= 0:
            raise ValueError("Empréstimo sem 'parcelas_total' ou 'valor_parcela' válido.")

        base_str = d.get("data_inicio_pagamento") or d.get("data_contratacao")
        if not base_str:
            base = date.today()
        else:
            base = datetime.strptime(base_str[:10], "%Y-%m-%d").date()

        venc_dia = int(d.get("vencimento_dia") or 0)
        if venc_dia <= 0:
            venc_dia = base.day  # fallback

        criadas = 0
        marcadas_quitadas = 0
        obrigacoes_ids = []

        for p in range(1, total_parc + 1):
            # calcula vencimento da parcela p
            vcto_mes = self._add_months(base.replace(day=1), p - 1)
            last_day = calendar.monthrange(vcto_mes.year, vcto_mes.month)[1]
            from datetime import date as _date
            venc_dt = _date(vcto_mes.year, vcto_mes.month, min(venc_dia, last_day))
            venc_str = venc_dt.strftime("%Y-%m-%d")

            # gera um novo obrigacao_id para esta parcela
            obrig_id = self.proximo_obrigacao_id(conn)

            # cria o LANCAMENTO
            lancamento_id = self.registrar_lancamento(
                conn,
                obrigacao_id=obrig_id,
                tipo_obrigacao="EMPRESTIMO",
                valor_total=float(vparc),
                data_evento=base.strftime("%Y-%m-%d"),
                vencimento=venc_str,
                descricao=f"Parcela {p}/{total_parc}",
                credor=credor,
                competencia=venc_str[:7],
                parcela_num=p,
                parcelas_total=total_parc,
                usuario=usuario,
            )
            criadas += 1
            obrigacoes_ids.append(obrig_id)

            # vincula origem
            if emprestimo_id is not None:
                conn.execute(
                    """
                    UPDATE contas_a_pagar_mov
                    SET tipo_origem = 'EMPRESTIMO',
                        emprestimo_id = ?
                    WHERE id = ?
                    """,
                    (int(emprestimo_id), int(lancamento_id)),
                )
            else:
                conn.execute(
                    """
                    UPDATE contas_a_pagar_mov
                    SET tipo_origem = 'EMPRESTIMO',
                        emprestimo_id = NULL
                    WHERE id = ?
                    """,
                    (int(lancamento_id),),
                )

            if p <= ja_pagas:
                # quitada: aplica pagamento “dentro” do próprio lançamento (status vira Quitado)
                self.aplicar_pagamento_parcela(
                    conn,
                    parcela_id=int(lancamento_id),
                    valor_parcela=float(vparc),
                    valor_pago_total=float(vparc),
                    juros=0.0,
                    multa=0.0,
                    desconto=0.0,
                )
                marcadas_quitadas += 1
            else:
                # força status 'Em aberto'
                conn.execute(
                    "UPDATE contas_a_pagar_mov SET status = 'Em aberto' WHERE id = ?",
                    (int(lancamento_id),),
                )

        return {
            "criadas": criadas,
            "ajustes_quitadas": marcadas_quitadas,
            "obrigacoes": obrigacoes_ids,
        }


# API pública explícita
__all__ = ["LoansMixin"]
