from __future__ import annotations

import sqlite3
from typing import Optional, Literal
import pandas as pd

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