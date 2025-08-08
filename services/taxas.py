import sqlite3
import pandas as pd

class TaxaMaquinetaManager:
    def __init__(self, caminho_banco: str):
        self.caminho_banco = caminho_banco
        self._criar_tabela()

    def _criar_tabela(self):
        with sqlite3.connect(self.caminho_banco) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS taxas_maquinas (
                    maquineta TEXT NOT NULL,
                    forma_pagamento TEXT NOT NULL,
                    bandeira TEXT NOT NULL,
                    parcelas INTEGER NOT NULL,
                    taxa_percentual REAL NOT NULL,
                    banco_destino TEXT,  -- NOVA COLUNA
                    PRIMARY KEY (maquineta, forma_pagamento, bandeira, parcelas)
                )
            """)
    
    def salvar_taxa(self, maquineta: str, forma: str, bandeira: str, parcelas: int, taxa: float, banco_destino: str):
        with sqlite3.connect(self.caminho_banco) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO taxas_maquinas 
                (maquineta, forma_pagamento, bandeira, parcelas, taxa_percentual, banco_destino)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (maquineta.upper(), forma.upper(), bandeira.upper(), parcelas, taxa, banco_destino))
            conn.commit()

    def carregar_taxas(self) -> pd.DataFrame:
        with sqlite3.connect(self.caminho_banco) as conn:
            df = pd.read_sql("""
                SELECT 
                    UPPER(maquineta) AS 'Maquineta',
                    UPPER(forma_pagamento) AS 'Forma de Pagamento', 
                    UPPER(bandeira) AS 'Bandeira',
                    parcelas AS 'Parcelas',
                    taxa_percentual AS 'Taxa (%)'
                FROM taxas_maquinas
                ORDER BY maquineta, forma_pagamento, bandeira, parcelas
            """, conn)
        return df