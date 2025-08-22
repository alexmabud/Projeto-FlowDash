"""
Módulo ContasAPagarMovRepository
================================

Este módulo define a classe `ContasAPagarMovRepository`, responsável por gerenciar
a tabela `contas_a_pagar_mov` no banco de dados SQLite. Ele centraliza o controle
de obrigações financeiras (parceladas ou programadas), permitindo registrar,
atualizar e consultar movimentações vinculadas a contas a pagar.

Funcionalidades principais
--------------------------
- Criação automática do schema da tabela `contas_a_pagar_mov`.
- Registro de novas parcelas/obrigações financeiras.
- Atualização de status de pagamento (pendente, parcial, quitado).
- Consulta de lançamentos por credor, competência, vencimento ou status.
- Suporte a movimentações originadas de cartões de crédito, boletos e empréstimos.

Detalhes técnicos
-----------------
- Conexão SQLite configurada em modo WAL, com busy_timeout e suporte a
  foreign keys.
- Registro idempotente para evitar duplicidade em lançamentos de parcelas.
- Métodos preparados para integração com serviços como `LedgerService`
  e repositórios de cartões e movimentações.

Dependências
------------
- sqlite3
- pandas
- typing (Optional, Dict, List)
- datetime (date)

"""

from __future__ import annotations

import sqlite3
from typing import Optional, Literal
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from datetime import date, datetime
import calendar

# ========= Helpers de arredondamento (nível de módulo) =========
def _q2(x) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

# ========= Tipos e conjuntos permitidos =========
TipoObrigacao = Literal["BOLETO", "FATURA_CARTAO", "EMPRESTIMO", "OUTRO"]
ALLOWED_TIPOS = {"BOLETO", "FATURA_CARTAO", "EMPRESTIMO", "OUTRO"}
ALLOWED_CATEGORIAS = {"LANCAMENTO", "PAGAMENTO", "JUROS", "MULTA", "DESCONTO", "AJUSTE", "CANCELAMENTO"}


class ContasAPagarMovRepository:
    """
    Repository para a tabela central 'contas_a_pagar_mov'.
    - Inserção de eventos (LANCAMENTO/PAGAMENTO/…)
    - Geração de novos obrigacao_id
    - Listagens e saldos para a UI (em_aberto, saldo de uma obrigação)
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    # ------------------ helpers internos ------------------

    def _validar_evento_basico(
        self,
        *,
        obrigacao_id: int,
        tipo_obrigacao: str,
        categoria_evento: str,
        data_evento: str,
        valor_evento: float,
        usuario: str
    ) -> None:
        if not isinstance(obrigacao_id, int):
            raise ValueError("obrigacao_id deve ser int.")
        if tipo_obrigacao not in ALLOWED_TIPOS:
            raise ValueError(f"tipo_obrigacao inválido: {tipo_obrigacao}. Use {sorted(ALLOWED_TIPOS)}")
        if categoria_evento not in ALLOWED_CATEGORIAS:
            raise ValueError(f"categoria_evento inválida: {categoria_evento}. Use {sorted(ALLOWED_CATEGORIAS)}")
        if not data_evento or len(data_evento) < 8:
            raise ValueError("data_evento deve ser 'YYYY-MM-DD'.")
        if float(valor_evento) == 0:
            raise ValueError("valor_evento deve ser diferente de zero.")
        if not usuario:
            raise ValueError("usuario é obrigatório.")

    def _inserir_evento(self, conn: sqlite3.Connection, **ev) -> int:
        """
        Insere um evento na tabela central. Espera que os campos já tenham sido validados.
        Preenche colunas opcionais com None quando não informadas.
        """
        cols = [
            "obrigacao_id", "tipo_obrigacao", "categoria_evento", "data_evento", "vencimento",
            "valor_evento", "descricao", "credor", "competencia", "parcela_num", "parcelas_total",
            "forma_pagamento", "origem", "ledger_id", "usuario"
        ]
        sql = f"INSERT INTO contas_a_pagar_mov ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
        cur = conn.cursor()
        cur.execute(sql, [ev.get(c) for c in cols])
        return int(cur.lastrowid)

    # ------------------ geração de IDs ------------------

    def proximo_obrigacao_id(self, conn: sqlite3.Connection) -> int:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(obrigacao_id), 0) + 1 FROM contas_a_pagar_mov;")
        return int(cur.fetchone()[0])

    # ------------------ inserções de eventos ------------------

    def registrar_lancamento(
        self,
        conn: sqlite3.Connection,
        *,
        obrigacao_id: int,
        tipo_obrigacao: TipoObrigacao,
        valor_total: float,
        data_evento: str,                 # 'YYYY-MM-DD'
        vencimento: Optional[str],        # 'YYYY-MM-DD' (p/ boleto/fatura/parcela)
        descricao: Optional[str],
        credor: Optional[str],
        competencia: Optional[str],       # 'YYYY-MM' (se None, tenta derivar de 'vencimento')
        parcela_num: Optional[int],
        parcelas_total: Optional[int],
        usuario: str
    ) -> int:
        valor_total = float(valor_total)
        if valor_total <= 0:
            raise ValueError("LANCAMENTO deve ter valor > 0.")

        # Deriva competência de 'vencimento' se não informada (ex.: '2025-08')
        competencia = competencia or (vencimento[:7] if vencimento else None)

        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao=tipo_obrigacao,
            categoria_evento="LANCAMENTO",
            data_evento=data_evento,
            valor_evento=valor_total,
            usuario=usuario,
        )

        return self._inserir_evento(
            conn,
            obrigacao_id=obrigacao_id,
            tipo_obrigacao=tipo_obrigacao,
            categoria_evento="LANCAMENTO",
            data_evento=data_evento,
            vencimento=vencimento,
            valor_evento=valor_total,     # LANCAMENTO é positivo
            descricao=descricao,
            credor=credor,
            competencia=competencia,
            parcela_num=parcela_num,
            parcelas_total=parcelas_total,
            forma_pagamento=None,
            origem=None,
            ledger_id=None,
            usuario=usuario,
        )

    def registrar_pagamento(
        self,
        conn: sqlite3.Connection,
        *,
        obrigacao_id: int,
        tipo_obrigacao: TipoObrigacao,
        valor_pago: float,
        data_evento: str,                 # 'YYYY-MM-DD'
        forma_pagamento: str,
        origem: str,
        ledger_id: int,
        usuario: str
    ) -> int:
        valor_pago = float(valor_pago)
        if valor_pago <= 0:
            raise ValueError("valor_pago deve ser > 0 para PAGAMENTO.")

        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao=tipo_obrigacao,
            categoria_evento="PAGAMENTO",
            data_evento=data_evento,
            valor_evento=-valor_pago,     # evento armazenado como negativo
            usuario=usuario,
        )

        return self._inserir_evento(
            conn,
            obrigacao_id=obrigacao_id,
            tipo_obrigacao=tipo_obrigacao,
            categoria_evento="PAGAMENTO",
            data_evento=data_evento,
            vencimento=None,
            valor_evento=-abs(valor_pago),  # PAGAMENTO é negativo
            descricao=None,
            credor=None,
            competencia=None,
            parcela_num=None,
            parcelas_total=None,
            forma_pagamento=forma_pagamento,
            origem=origem,
            ledger_id=int(ledger_id),
            usuario=usuario,
        )

    def registrar_ajuste_legado(
        self,
        conn: sqlite3.Connection,
        *,
        obrigacao_id: int,
        tipo_obrigacao: TipoObrigacao,
        valor_negativo: float,            # informe valor POSITIVO; será aplicado como negativo
        data_evento: str,
        descricao: Optional[str],
        credor: Optional[str],
        usuario: str
    ) -> int:
        """
        Use para importar 'passado pago' (empréstimos antigos etc.):
        cria um evento AJUSTE NEGATIVO (não mexe em caixa, ledger_id=None).
        """
        valor_negativo = float(valor_negativo)
        if valor_negativo <= 0:
            raise ValueError("valor_negativo deve ser > 0 (será gravado como negativo).")

        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao=tipo_obrigacao,
            categoria_evento="AJUSTE",
            data_evento=data_evento,
            valor_evento=-valor_negativo,
            usuario=usuario,
        )

        return self._inserir_evento(
            conn,
            obrigacao_id=obrigacao_id,
            tipo_obrigacao=tipo_obrigacao,
            categoria_evento="AJUSTE",
            data_evento=data_evento,
            vencimento=None,
            valor_evento=-abs(valor_negativo),
            descricao=descricao,
            credor=credor,
            competencia=None,
            parcela_num=None,
            parcelas_total=None,
            forma_pagamento="LEGADO",
            origem="IMPORTACAO",
            ledger_id=None,
            usuario=usuario,
        )

    # ------------------ consultas para a UI ------------------

    def listar_em_aberto(self, conn: sqlite3.Connection, tipo_obrigacao: str | None = None) -> pd.DataFrame:
        """
        Retorna obrigações em aberto a partir de vw_cap_em_aberto.
        Se tipo_obrigacao vier ('BOLETO'|'FATURA_CARTAO'|'EMPRESTIMO'), filtra por tipo.
        """
        if tipo_obrigacao:
            sql = """
                SELECT obrigacao_id, tipo_obrigacao, credor, descricao, competencia, vencimento,
                       total_lancado, total_pago, saldo_aberto, perc_quitado
                FROM vw_cap_em_aberto
                WHERE tipo_obrigacao = ?
                ORDER BY date(vencimento) ASC NULLS LAST, obrigacao_id ASC;
            """
            return pd.read_sql(sql, conn, params=(tipo_obrigacao,))
        else:
            sql = """
                SELECT obrigacao_id, tipo_obrigacao, credor, descricao, competencia, vencimento,
                       total_lancado, total_pago, saldo_aberto, perc_quitado
                FROM vw_cap_em_aberto
                ORDER BY date(vencimento) ASC NULLS LAST, tipo_obrigacao, obrigacao_id ASC;
            """
            return pd.read_sql(sql, conn)

    def obter_saldo_obrigacao(self, conn: sqlite3.Connection, obrigacao_id: int) -> float:
        """
        Retorna o saldo em aberto (ou 0 se não existir) a partir de vw_cap_saldos.
        """
        row = conn.execute(
            "SELECT COALESCE(saldo_aberto,0) FROM vw_cap_saldos WHERE obrigacao_id=?;",
            (obrigacao_id,)
        ).fetchone()
        return float(row[0]) if row else 0.0

    # ------------------ ajustes (multa/juros/desconto) p/ BOLETO ------------------

    def registrar_multa_boleto(self, conn, *, obrigacao_id: int, valor: float, data_evento: str, usuario: str, descricao: str | None = None) -> int:
        v = float(valor)
        if v <= 0:
            return 0
        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao="BOLETO",
            categoria_evento="MULTA",
            data_evento=data_evento,
            valor_evento=v,
            usuario=usuario
        )
        return self._inserir_evento(
            conn,
            obrigacao_id=obrigacao_id, tipo_obrigacao="BOLETO", categoria_evento="MULTA",
            data_evento=data_evento, vencimento=None, valor_evento=v, descricao=descricao,
            credor=None, competencia=None, parcela_num=None, parcelas_total=None,
            forma_pagamento=None, origem=None, ledger_id=None, usuario=usuario
        )

    def registrar_juros_boleto(self, conn, *, obrigacao_id: int, valor: float, data_evento: str, usuario: str, descricao: str | None = None) -> int:
        v = float(valor)
        if v <= 0:
            return 0
        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao="BOLETO",
            categoria_evento="JUROS",
            data_evento=data_evento,
            valor_evento=v,
            usuario=usuario
        )
        return self._inserir_evento(
            conn,
            obrigacao_id=obrigacao_id, tipo_obrigacao="BOLETO", categoria_evento="JUROS",
            data_evento=data_evento, vencimento=None, valor_evento=v, descricao=descricao,
            credor=None, competencia=None, parcela_num=None, parcelas_total=None,
            forma_pagamento=None, origem=None, ledger_id=None, usuario=usuario
        )

    def registrar_desconto_boleto(self, conn, *, obrigacao_id: int, valor: float, data_evento: str, usuario: str, descricao: str | None = None) -> int:
        v = float(valor)
        if v <= 0:
            return 0
        # desconto reduz a dívida (evento negativo)
        self._validar_evento_basico(
            obrigacao_id=obrigacao_id,
            tipo_obrigacao="BOLETO",
            categoria_evento="DESCONTO",
            data_evento=data_evento,
            valor_evento=-v,
            usuario=usuario
        )
        return self._inserir_evento(
            conn,
            obrigacao_id=obrigacao_id, tipo_obrigacao="BOLETO", categoria_evento="DESCONTO",
            data_evento=data_evento, vencimento=None, valor_evento=-v, descricao=descricao,
            credor=None, competencia=None, parcela_num=None, parcelas_total=None,
            forma_pagamento=None, origem=None, ledger_id=None, usuario=usuario
        )

    # ------------------ validação de pagamento vs saldo ------------------

    def _validar_pagamento_nao_excede_saldo(
        self,
        conn: sqlite3.Connection,
        obrigacao_id: int,
        valor_pago: float
    ) -> float:
        """
        Garante que o pagamento não exceda o saldo em aberto.
        Retorna o saldo atual. Lança ValueError se exceder (com tolerância de centavos).
        """
        saldo = float(self.obter_saldo_obrigacao(conn, int(obrigacao_id)))
        valor_pago = float(valor_pago)
        if valor_pago <= 0:
            raise ValueError("O valor do pagamento deve ser positivo.")
        # Tolerância para arredondamentos
        eps = 0.005
        if valor_pago > saldo + eps:
            raise ValueError(f"Pagamento (R$ {valor_pago:.2f}) maior que o saldo (R$ {saldo:.2f}).")
        return saldo

    # ------------------ pagamento de parcela: BOLETO ------------------

    def registrar_pagamento_parcela_boleto(
        self,
        conn: sqlite3.Connection,
        *,
        obrigacao_id: int,
        valor_pago: float,
        data_evento: str,          # 'YYYY-MM-DD'
        forma_pagamento: str,
        origem: str,               # 'Caixa' / 'Caixa 2' / nome do banco
        ledger_id: int,
        usuario: str,
        descricao_extra: Optional[str] = None
    ) -> int:
        """
        Insere um evento PAGAMENTO (valor_evento negativo) para um boleto (tipo_obrigacao='BOLETO'),
        vinculado ao obrigacao_id informado. Valida para não exceder o saldo.
        Retorna o ID do evento inserido.
        """
        # 1) valida saldo
        self._validar_pagamento_nao_excede_saldo(conn, int(obrigacao_id), float(valor_pago))

        # 2) validações básicas do evento (usa tipo BOLETO e categoria PAGAMENTO)
        self._validar_evento_basico(
            obrigacao_id=int(obrigacao_id),
            tipo_obrigacao="BOLETO",
            categoria_evento="PAGAMENTO",
            data_evento=data_evento,
            valor_evento=-abs(float(valor_pago)),
            usuario=usuario,
        )

        # 3) insere evento (PAGAMENTO é negativo)
        return self._inserir_evento(
            conn,
            obrigacao_id=int(obrigacao_id),
            tipo_obrigacao="BOLETO",
            categoria_evento="PAGAMENTO",
            data_evento=data_evento,
            vencimento=None,
            valor_evento=-abs(float(valor_pago)),
            descricao=descricao_extra,     # dica: "Parcela 2/5 — Credor X"
            credor=None,
            competencia=None,
            parcela_num=None,
            parcelas_total=None,
            forma_pagamento=forma_pagamento,
            origem=origem,
            ledger_id=int(ledger_id) if ledger_id is not None else None,
            usuario=usuario,
        )

    # ------------------ listagem detalhada de boletos (parcelas) ------------------

    def listar_boletos_em_aberto_detalhado(self, conn: sqlite3.Connection, credor: str | None = None) -> pd.DataFrame:
        """
        Lista parcelas (LANCAMENTOS) de BOLETO em aberto/parcial, calculando saldo pela própria tabela de eventos.
        Não depende de colunas extras (status/valor_pago_acumulado). Status é derivado:
          - Quitado: saldo <= 0
          - Parcial: saldo > 0 e total_pago > 0
          - Em aberto: saldo > 0 e total_pago = 0
        """
        base_sql = """
            WITH base AS (
              SELECT
                cap.obrigacao_id,
                MIN(COALESCE(cap.credor, '')) AS credor,
                MIN(COALESCE(cap.descricao, '')) AS descricao,
                MIN(cap.parcela_num) AS parcela_num,
                MIN(cap.parcelas_total) AS parcelas_total,
                MIN(cap.vencimento) AS vencimento,
                SUM(CASE WHEN cap.categoria_evento='LANCAMENTO' THEN cap.valor_evento ELSE 0 END) AS total_lancado,
                SUM(CASE WHEN cap.categoria_evento='PAGAMENTO'  THEN -cap.valor_evento ELSE 0 END) AS total_pago,
                SUM(CASE 
                      WHEN cap.categoria_evento='MULTA'    THEN cap.valor_evento
                      WHEN cap.categoria_evento='JUROS'    THEN cap.valor_evento
                      WHEN cap.categoria_evento='DESCONTO' THEN cap.valor_evento  -- já negativo
                      WHEN cap.categoria_evento='AJUSTE'   THEN cap.valor_evento  -- legado (pode ser negativo)
                      ELSE 0 END
                ) AS total_ajustes
              FROM contas_a_pagar_mov cap
              WHERE cap.tipo_obrigacao='BOLETO'
              GROUP BY cap.obrigacao_id
            )
            SELECT
              obrigacao_id,
              credor,
              descricao,
              parcela_num,
              parcelas_total,
              vencimento,
              ROUND(total_lancado, 2) AS valor_parcela,
              ROUND(total_lancado + COALESCE(total_ajustes,0) - COALESCE(total_pago,0), 2) AS saldo,
              CASE
                WHEN (total_lancado + COALESCE(total_ajustes,0) - COALESCE(total_pago,0)) <= 0.00001 THEN 'Quitado'
                WHEN COALESCE(total_pago,0) > 0 THEN 'Parcial'
                ELSE 'Em aberto'
              END AS status
            FROM base
            WHERE (total_lancado + COALESCE(total_ajustes,0) - COALESCE(total_pago,0)) > 0.00001
            {filtro_credor}
            ORDER BY DATE(COALESCE(vencimento, DATE('now'))) ASC, parcela_num ASC, obrigacao_id ASC;
        """
        sql = base_sql.format(
            filtro_credor="AND LOWER(TRIM(credor)) = LOWER(TRIM(?))" if credor else ""
        )
        if credor:
            return pd.read_sql(sql, conn, params=(credor,))
        else:
            return pd.read_sql(sql, conn)

    # ------------------ NOVO: aplicar pagamento na própria parcela ------------------

    def aplicar_pagamento_parcela(
        self,
        conn: sqlite3.Connection,
        *,
        parcela_id: int,
        valor_parcela: float,
        valor_pago_total: float,   # total desembolsado agora (já com juros/multa/desconto aplicados)
        juros: float = 0.0,
        multa: float = 0.0,
        desconto: float = 0.0,
    ) -> dict:
        """
        Atualiza a própria linha da parcela acumulando pagamento/encargos e define o status.
        (Use somente se sua tabela possuir essas colunas extras; caso contrário, prefira o modelo de eventos+views.)
        Regra: valor_quitacao = valor_parcela - desconto + juros + multa
               Quitado se valor_pago_acumulado >= valor_quitacao
        """
        vp   = _q2(valor_parcela)
        pago = _q2(valor_pago_total)
        j    = _q2(juros)
        m    = _q2(multa)
        d    = _q2(desconto)

        valor_quitacao = _q2(vp - d + j + m)

        cur = conn.cursor()
        cur.execute("""
            SELECT 
                COALESCE(valor_pago_acumulado,0),
                COALESCE(juros_pago,0),
                COALESCE(multa_paga,0),
                COALESCE(desconto_aplicado,0)
            FROM contas_a_pagar_mov
            WHERE id = ?
        """, (parcela_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Parcela id={parcela_id} não encontrada em contas_a_pagar_mov")

        pago_acum_atual, juros_acum_atual, multa_acum_atual, desc_acum_atual = map(Decimal, map(str, row))

        novo_pago_acum = _q2(pago_acum_atual + pago)
        novo_juros     = _q2(juros_acum_atual + j)
        novo_multa     = _q2(multa_acum_atual + m)
        novo_desc      = _q2(desc_acum_atual + d)

        status = "Quitado" if novo_pago_acum >= valor_quitacao else "Parcial"
        restante = _q2(max(Decimal("0.00"), valor_quitacao - novo_pago_acum))

        cur.execute("""
            UPDATE contas_a_pagar_mov
               SET valor_pago_acumulado = ?,
                   juros_pago           = ?,
                   multa_paga           = ?,
                   desconto_aplicado    = ?,
                   status               = ?
             WHERE id = ?
        """, (
            float(novo_pago_acum),
            float(novo_juros),
            float(novo_multa),
            float(novo_desc),
            status,
            parcela_id
        ))
        conn.commit()

        return {
            "parcela_id": parcela_id,
            "valor_parcela": float(vp),
            "valor_quitacao": float(valor_quitacao),
            "pago_acumulado": float(novo_pago_acum),
            "status": status,
            "restante": float(restante)
        }

    # ------------------ helpers de empréstimo ------------------

    def _label_emprestimo(self, row) -> str:
        """
        Define o 'credor' que aparecerá em contas_a_pagar_mov:
        prioriza banco; cai para descricao; depois tipo.
        """
        for k in ("banco", "descricao", "tipo"):
            v = (row.get(k) or "").strip()
            if v:
                return v
        return "Empréstimo"

    def _add_months(self, d: date, months: int) -> date:
        y = d.year + (d.month - 1 + months) // 12
        m = (d.month - 1 + months) % 12 + 1
        last = calendar.monthrange(y, m)[1]
        day = min(d.day, last)
        return date(y, m, day)

    # ------------------ geração de parcelas de EMPRESTIMO ------------------

    def gerar_parcelas_emprestimo(
    self,
    conn: sqlite3.Connection,
    *,
    emprestimo_id: int,
    usuario: str
    ) -> dict:
        """
        Cria LANCAMENTOS (tipo_obrigacao='EMPRESTIMO') para todas as parcelas do empréstimo.
        Para as primeiras 'parcelas_pagas', NÃO cria evento extra: aplica o pagamento
        diretamente na própria linha do LANCAMENTO (acumuladores + status=Quitado).
        Nas demais, força status='Em aberto'.
        Em TODAS as parcelas, define tipo_origem='EMPRESTIMO' e emprestimo_id (quando houver).
        Não movimenta caixa.
        """
        # Carrega dados do empréstimo
        row = conn.execute("""
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
        """, (int(emprestimo_id),)).fetchone()
        if not row:
            raise ValueError(f"Empréstimo id={emprestimo_id} não encontrado.")

        colunas = [
            "id", "banco", "descricao", "tipo",
            "parcelas_total", "parcelas_pagas", "valor_parcela",
            "data_inicio_pagamento", "data_contratacao", "vencimento_dia"
        ]
        d = dict(zip(colunas, row))

        total_parc = int(d.get("parcelas_total") or 0)
        ja_pagas   = max(0, min(total_parc, int(d.get("parcelas_pagas") or 0)))
        vparc      = float(d.get("valor_parcela") or 0.0)
        credor     = self._label_emprestimo(d)

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
            venc_dt  = date(vcto_mes.year, vcto_mes.month, min(venc_dia, last_day))
            venc_str = venc_dt.strftime("%Y-%m-%d")

            # gera um novo obrigacao_id para esta parcela
            obrig_id = self.proximo_obrigacao_id(conn)

            # cria o LANCAMENTO (retorna o 'id' da linha criada)
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
                usuario=usuario
            )
            criadas += 1
            obrigacoes_ids.append(obrig_id)

            # 🔹 SEMPRE: tipar origem e vincular o emprestimo_id
            if emprestimo_id is not None:
                conn.execute("""
                    UPDATE contas_a_pagar_mov
                    SET tipo_origem = 'EMPRESTIMO',
                        emprestimo_id = ?
                    WHERE id = ?
                """, (int(emprestimo_id), int(lancamento_id)))
            else:
                conn.execute("""
                    UPDATE contas_a_pagar_mov
                    SET tipo_origem = 'EMPRESTIMO',
                        emprestimo_id = NULL
                    WHERE id = ?
                """, (int(lancamento_id),))

            if p <= ja_pagas:
                # quitada: aplica pagamento “dentro” do próprio lançamento (status vira Quitado)
                self.aplicar_pagamento_parcela(
                    conn,
                    parcela_id=int(lancamento_id),
                    valor_parcela=float(vparc),
                    valor_pago_total=float(vparc),
                    juros=0.0,
                    multa=0.0,
                    desconto=0.0
                )
                marcadas_quitadas += 1
            else:
                # NÃO quitada: força status = 'Em aberto'
                conn.execute("""
                    UPDATE contas_a_pagar_mov
                    SET status = 'Em aberto'
                    WHERE id = ?
                """, (int(lancamento_id),))

        return {
            "criadas": criadas,
            "ajustes_quitadas": marcadas_quitadas,
            "obrigacoes": obrigacoes_ids
        }