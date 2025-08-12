import sqlite3
import pandas as pd
from typing import Optional, Dict, Any, List, Tuple

# === Classe Usuário ========================================================================================
class Usuario:
    def __init__(self, id: int, nome: str, email: str, perfil: str, ativo: int):
        self.id = id
        self.nome = nome
        self.email = email
        self.perfil = perfil
        self.ativo = ativo

    def exibir_info(self) -> Tuple[str, str, str]:
        status = "🟢 Ativo" if self.ativo == 1 else "🔴 Inativo"
        return self.nome, self.email, status

    def alternar_status(self, caminho_banco: str) -> None:
        novo_status = 0 if self.ativo == 1 else 1
        with sqlite3.connect(caminho_banco) as conn:
            conn.execute("UPDATE usuarios SET ativo = ? WHERE id = ?", (novo_status, self.id))
            conn.commit()

    def excluir(self, caminho_banco: str) -> None:
        with sqlite3.connect(caminho_banco) as conn:
            conn.execute("DELETE FROM usuarios WHERE id = ?", (self.id,))
            conn.commit()


# === Classe MetaManager ====================================================================================
DIAS_SEMANA = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]

class MetaManager:
    def __init__(self, caminho_banco: str):
        self.caminho_banco = caminho_banco

    def carregar_usuarios_ativos(self) -> List[Tuple[str, int]]:
        with sqlite3.connect(self.caminho_banco) as conn:
            df = conn.execute("SELECT id, nome FROM usuarios WHERE ativo = 1").fetchall()
            return [("LOJA", 0)] + [(nome, id) for id, nome in df]

    def salvar_meta(self, id_usuario: int, vendedor: str, mensal: float, semanal_percentual: float,
                    dias_percentuais: List[float], perc_bronze: float, perc_prata: float, mes: str) -> bool:
        with sqlite3.connect(self.caminho_banco) as conn:
            cursor = conn.execute("SELECT 1 FROM metas WHERE id_usuario = ? AND mes = ?", (id_usuario, mes))
            existe = cursor.fetchone()

            if existe:
                conn.execute("""UPDATE metas SET 
                    vendedor = ?, perc_segunda = ?, perc_terca = ?, perc_quarta = ?, perc_quinta = ?, perc_sexta = ?, 
                    perc_sabado = ?, perc_domingo = ?, perc_semanal = ?, meta_mensal = ?, perc_bronze = ?, perc_prata = ?
                    WHERE id_usuario = ? AND mes = ?""",
                    (vendedor.upper(), *dias_percentuais, semanal_percentual, mensal, perc_bronze, perc_prata, id_usuario, mes))
            else:
                cursor = conn.execute("""SELECT perc_segunda, perc_terca, perc_quarta, perc_quinta, perc_sexta,
                                                perc_sabado, perc_domingo, perc_semanal, meta_mensal,
                                                perc_bronze, perc_prata
                                         FROM metas WHERE id_usuario = ? AND mes < ?
                                         ORDER BY mes DESC LIMIT 1""", (id_usuario, mes))
                meta_anterior = cursor.fetchone()
                if meta_anterior:
                    valores = meta_anterior
                else:
                    valores = (*dias_percentuais, semanal_percentual, mensal, perc_bronze, perc_prata)

                conn.execute("""INSERT INTO metas (
                    id_usuario, vendedor, perc_segunda, perc_terca, perc_quarta, perc_quinta, perc_sexta,
                    perc_sabado, perc_domingo, perc_semanal, meta_mensal, perc_bronze, perc_prata, mes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (id_usuario, vendedor.upper(), *valores, mes))

            conn.commit()
            return True

    def carregar_metas_cadastradas(self) -> List[dict]:
        with sqlite3.connect(self.caminho_banco) as conn:
            df = conn.execute("""SELECT COALESCE(u.nome, m.vendedor, 'LOJA') AS Vendedor, m.mes,
                                        m.meta_mensal, m.perc_semanal, m.perc_prata, m.perc_bronze,
                                        m.perc_segunda, m.perc_terca, m.perc_quarta, m.perc_quinta,
                                        m.perc_sexta, m.perc_sabado, m.perc_domingo
                                 FROM metas m LEFT JOIN usuarios u ON m.id_usuario = u.id
                                 ORDER BY m.mes DESC, Vendedor""").fetchall()
            colunas = ["Vendedor", "Mês", "Meta Mensal", "Meta Semanal", "% Prata", "% Bronze"] + DIAS_SEMANA
            return [dict(zip(colunas, linha)) for linha in df]


# === Classe Cartão de Crédito ===================================================================================
class CartaoCredito:
    def __init__(self, nome: str, fechamento: int, vencimento: int):
        self.nome = nome.strip()
        self.fechamento = fechamento
        self.vencimento = vencimento

    def salvar(self, caminho_banco: str) -> None:
        with sqlite3.connect(caminho_banco) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cartoes_credito (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome TEXT NOT NULL,
                    fechamento INTEGER NOT NULL,
                    vencimento INTEGER
                )
            """)
            conn.execute("""
                INSERT INTO cartoes_credito (nome, fechamento, vencimento)
                VALUES (?, ?, ?)
            """, (self.nome, self.fechamento, self.vencimento))
            conn.commit()

# === Classe CaixaRepository ===================================================================================
class CaixaRepository:
    def __init__(self, caminho_banco: str):
        self.caminho_banco = caminho_banco

    def buscar_saldo_por_data(self, data: str):
        with sqlite3.connect(self.caminho_banco) as conn:
            cursor = conn.execute("SELECT caixa, caixa_2 FROM saldos_caixas WHERE data = ?", (data,))
            return cursor.fetchone()

    def salvar_saldo(self, data: str, caixa: float, caixa_2: float, atualizar=False):
        with sqlite3.connect(self.caminho_banco) as conn:
            if atualizar:
                conn.execute("""
                    UPDATE saldos_caixas
                    SET caixa = ?, caixa_2 = ?
                    WHERE data = ?
                """, (caixa, caixa_2, data))
            else:
                conn.execute("""
                    INSERT INTO saldos_caixas (data, caixa, caixa_2)
                    VALUES (?, ?, ?)
                """, (data, caixa, caixa_2))
            conn.commit()

    def listar_ultimos_saldos(self, limite=15):
        with sqlite3.connect(self.caminho_banco) as conn:
            return pd.read_sql(f"""
                SELECT data, caixa, caixa_2 
                FROM saldos_caixas 
                ORDER BY data DESC 
                LIMIT {limite}
            """, conn)
        

# === Classe CorrecaoCaixaRepository ============================================================================
class CorrecaoCaixaRepository:
    def __init__(self, caminho_banco: str):
        self.caminho_banco = caminho_banco

    def salvar_ajuste(self, data_: str, valor: float, observacao: str) -> int:
        with sqlite3.connect(self.caminho_banco) as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO correcao_caixa (data, valor, observacao)
                VALUES (?, ?, ?)
            """, (data_, valor, observacao))
            conn.commit()
            return cur.lastrowid  # <<< retorna o ID do ajuste

    def listar_ajustes(self) -> pd.DataFrame:
        with sqlite3.connect(self.caminho_banco) as conn:
            return pd.read_sql("SELECT * FROM correcao_caixa ORDER BY id DESC", conn)
    

# === Classe SaldoBancarioRepository ============================================================================
class SaldoBancarioRepository:
    def __init__(self, caminho_banco: str):
        self.caminho_banco = caminho_banco

    def obter_saldo_por_data(self, data: str) -> Optional[Tuple[float, float, float, float]]:
        with sqlite3.connect(self.caminho_banco) as conn:
            cursor = conn.execute(
                "SELECT banco_1, banco_2, banco_3, banco_4 FROM saldos_bancos WHERE data = ?",
                (data,)
            )
            return cursor.fetchone()

    def salvar_saldo(self, data: str, b1: float, b2: float, b3: float, b4: float):
        with sqlite3.connect(self.caminho_banco) as conn:
            conn.execute("""
                INSERT INTO saldos_bancos (data, banco_1, banco_2, banco_3, banco_4)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(data) DO UPDATE SET
                    banco_1=excluded.banco_1,
                    banco_2=excluded.banco_2,
                    banco_3=excluded.banco_3,
                    banco_4=excluded.banco_4
            """, (data, b1, b2, b3, b4))
            conn.commit()


# # === Classe EmprestimoRepository ============================================================================
class EmprestimoRepository:
    def __init__(self, caminho_banco: str):
        self.caminho_banco = caminho_banco

    def salvar_emprestimo(self, dados: tuple) -> int:
        with sqlite3.connect(self.caminho_banco) as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO emprestimos_financiamentos (
                    data_contratacao, valor_total, tipo, banco, parcelas_total,
                    parcelas_pagas, valor_parcela, taxa_juros_am, vencimento_dia,
                    status, usuario, data_quitacao, origem_recursos,
                    valor_pago, valor_em_aberto, renegociado_de, descricao,
                    data_inicio_pagamento, data_lancamento
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, dados)
            conn.commit()
            return cur.lastrowid

    def listar_emprestimos(self) -> pd.DataFrame:
        with sqlite3.connect(self.caminho_banco) as conn:
            return pd.read_sql(
                "SELECT * FROM emprestimos_financiamentos ORDER BY id DESC",
                conn
            )

    def obter_emprestimo(self, id_: int) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(self.caminho_banco) as conn:
            df = pd.read_sql(
                "SELECT * FROM emprestimos_financiamentos WHERE id = ?",
                conn,
                params=(id_,)
            )
        return df.iloc[0].to_dict() if not df.empty else None

    def editar_emprestimo(self, id_emp: int, dados: Dict[str, Any]) -> None:
        """
        Atualiza um empréstimo recebendo um dict com as chaves == colunas.
        Campos esperados:
        data_contratacao, valor_total, tipo, banco, parcelas_total, parcelas_pagas,
        valor_parcela, taxa_juros_am, vencimento_dia, status, usuario,
        data_quitacao, origem_recursos, valor_pago, valor_em_aberto,
        renegociado_de, descricao, data_inicio_pagamento, data_lancamento
        """
        campos = [
            "data_contratacao", "valor_total", "tipo", "banco", "parcelas_total",
            "parcelas_pagas", "valor_parcela", "taxa_juros_am", "vencimento_dia",
            "status", "usuario", "data_quitacao", "origem_recursos", "valor_pago",
            "valor_em_aberto", "renegociado_de", "descricao",
            "data_inicio_pagamento", "data_lancamento"
        ]
        set_clause = ", ".join([f"{c}=?" for c in campos])
        valores = [dados.get(c) for c in campos]
        valores.append(id_emp)

        with sqlite3.connect(self.caminho_banco) as conn:
            conn.execute(
                f"UPDATE emprestimos_financiamentos SET {set_clause} WHERE id = ?",
                valores
            )
            conn.commit()

    def atualizar_emprestimo(self, id_: int, dados: tuple) -> None:
        with sqlite3.connect(self.caminho_banco) as conn:
            conn.execute("""
                UPDATE emprestimos_financiamentos SET
                    data_contratacao = ?, valor_total = ?, tipo = ?, banco = ?, parcelas_total = ?,
                    parcelas_pagas = ?, valor_parcela = ?, taxa_juros_am = ?, vencimento_dia = ?,
                    status = ?, usuario = ?, data_quitacao = ?, origem_recursos = ?,
                    valor_pago = ?, valor_em_aberto = ?, renegociado_de = ?, descricao = ?,
                    data_inicio_pagamento = ?, data_lancamento = ?
                WHERE id = ?
            """, dados + (id_,))
            conn.commit()

    def excluir_emprestimo(self, id_: int) -> None:
        with sqlite3.connect(self.caminho_banco) as conn:
            conn.execute("DELETE FROM emprestimos_financiamentos WHERE id = ?", (id_,))
            conn.commit()

# === Classe: Repositório de Bancos ==========================================================================
class BancoRepository:
    def __init__(self, caminho_banco: str):
        self.caminho_banco = caminho_banco
        self._criar_tabela_bancos()

    # Conexão padrão do projeto (estável com OneDrive)
    def _get_conn(self):
        conn = sqlite3.connect(self.caminho_banco, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _criar_tabela_bancos(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bancos_cadastrados (
                    id   INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome TEXT UNIQUE NOT NULL
                );
            """)
            conn.commit()

    def salvar_novo_banco(self, nome_banco: str):
        nome_banco = (nome_banco or "").strip()
        if not nome_banco:
            return
        with self._get_conn() as conn:
            # cadastra (ignora se já existir)
            conn.execute(
                "INSERT OR IGNORE INTO bancos_cadastrados (nome) VALUES (?)",
                (nome_banco,)
            )
            # garante coluna dinâmica em saldos_bancos
            cols = pd.read_sql("PRAGMA table_info(saldos_bancos);", conn)["name"].tolist()
            if nome_banco not in cols:
                conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{nome_banco}" REAL DEFAULT 0.0;')
            conn.commit()

    def carregar_bancos(self) -> pd.DataFrame:
        with self._get_conn() as conn:
            return pd.read_sql("SELECT id, nome FROM bancos_cadastrados ORDER BY nome", conn)

    def excluir_banco(self, banco_id: int):
        with self._get_conn() as conn:
            conn.execute("DELETE FROM bancos_cadastrados WHERE id = ?", (banco_id,))
            conn.commit()