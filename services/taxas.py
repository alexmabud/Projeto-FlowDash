"""
Módulo TaxaMaquinetaManager
===========================

Gerencia a tabela `taxas_maquinas` no SQLite para configurar **taxas por
maquineta/PSP** em diferentes combinações de forma de pagamento, bandeira
e parcelas. Também suporta um **banco de destino** para a liquidação.

Funcionalidades principais
--------------------------
- Garante o schema da tabela `taxas_maquinas`.
- Insere/atualiza taxas por combinação: (maquineta, forma, bandeira, parcelas).
- Carrega as taxas em `pandas.DataFrame` com rótulos amigáveis.

Tabela
------
`taxas_maquinas` (PK composta):
- maquineta TEXT
- forma_pagamento TEXT
- bandeira TEXT
- parcelas INTEGER
- taxa_percentual REAL
- banco_destino TEXT (opcional; banco que receberá a liquidação)

Dependências
------------
- sqlite3
- pandas
"""

from __future__ import annotations

import sqlite3
import pandas as pd

__all__ = ["TaxaMaquinetaManager"]


class TaxaMaquinetaManager:
    """CRUD mínimo para a tabela `taxas_maquinas`.

    Garante a existência da tabela no construtor e expõe operações
    para salvar (insert/replace) e carregar as taxas.
    """

    def __init__(self, caminho_banco: str) -> None:
        """Inicializa o gerenciador e assegura o schema.

        Args:
            caminho_banco: Caminho para o arquivo SQLite (.db).
        """
        self.caminho_banco = caminho_banco
        self._criar_tabela()

    def _criar_tabela(self) -> None:
        """Cria a tabela `taxas_maquinas` caso não exista.

        Colunas:
            - maquineta TEXT NOT NULL
            - forma_pagamento TEXT NOT NULL
            - bandeira TEXT NOT NULL
            - parcelas INTEGER NOT NULL
            - taxa_percentual REAL NOT NULL
            - banco_destino TEXT (opcional)

        Observação:
            A PK composta evita duplicidades por combinação de chaves.
        """
        with sqlite3.connect(self.caminho_banco) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS taxas_maquinas (
                    maquineta TEXT NOT NULL,
                    forma_pagamento TEXT NOT NULL,
                    bandeira TEXT NOT NULL,
                    parcelas INTEGER NOT NULL,
                    taxa_percentual REAL NOT NULL,
                    banco_destino TEXT,
                    PRIMARY KEY (maquineta, forma_pagamento, bandeira, parcelas)
                )
                """
            )

    def salvar_taxa(
        self,
        maquineta: str,
        forma: str,
        bandeira: str,
        parcelas: int,
        taxa: float,
        banco_destino: str,
    ) -> None:
        """Insere ou atualiza uma taxa de maquineta.

        Usa `INSERT OR REPLACE` sobre a PK composta.

        Args:
            maquineta: Nome da maquineta/PSP.
            forma: Forma de pagamento (ex.: PIX, DÉBITO, CRÉDITO, LINK_PAGAMENTO).
            bandeira: Bandeira do cartão (ou vazio quando não aplicável).
            parcelas: Número de parcelas (1 para à vista).
            taxa: Taxa percentual aplicada.
            banco_destino: Banco de liquidação associado (opcional).
        """
        with sqlite3.connect(self.caminho_banco) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO taxas_maquinas
                    (maquineta, forma_pagamento, bandeira, parcelas, taxa_percentual, banco_destino)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (maquineta.upper(), forma.upper(), bandeira.upper(), parcelas, taxa, banco_destino),
            )
            conn.commit()

    def carregar_taxas(self) -> pd.DataFrame:
        """Carrega as taxas cadastradas em um DataFrame.

        Retorna colunas com rótulos amigáveis e ordenação estável.

        Returns:
            DataFrame com os dados de `taxas_maquinas` prontos para exibição.
        """
        with sqlite3.connect(self.caminho_banco) as conn:
            df = pd.read_sql(
                """
                SELECT
                    UPPER(maquineta)        AS "Maquineta",
                    UPPER(forma_pagamento)  AS "Forma de Pagamento",
                    UPPER(bandeira)         AS "Bandeira",
                    parcelas                AS "Parcelas",
                    taxa_percentual         AS "Taxa (%)"
                FROM taxas_maquinas
                ORDER BY maquineta, forma_pagamento, bandeira, parcelas
                """,
                conn,
            )
        return df
