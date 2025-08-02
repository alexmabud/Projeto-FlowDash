
import sqlite3
import pandas as pd

# ============================
# Função Genérica
# ============================

def carregar_tabela(nome_tabela: str, caminho_banco: str) -> pd.DataFrame:
    """
    Carrega qualquer tabela do banco de dados SQLite como DataFrame.

    Args:
        nome_tabela (str): Nome da tabela.
        caminho_banco (str): Caminho do banco de dados .db.

    Returns:
        pd.DataFrame: Dados da tabela ou DataFrame vazio em caso de erro.
    """
    try:
        with sqlite3.connect(caminho_banco) as conn:
            return pd.read_sql(f"SELECT * FROM {nome_tabela}", conn)
    except Exception as e:
        print(f"[ERRO] Não foi possível carregar a tabela '{nome_tabela}': {e}")
        return pd.DataFrame()

# ============================
# Funções específicas por tabela
# ============================

def carregar_mercadorias(caminho_banco: str) -> pd.DataFrame:
    """Carrega a tabela de mercadorias."""
    return carregar_tabela("mercadorias", caminho_banco)

def carregar_usuarios(caminho_banco: str) -> pd.DataFrame:
    """Carrega os usuários do sistema."""
    return carregar_tabela("usuarios", caminho_banco)

def carregar_correcoes_caixa(caminho_banco: str) -> pd.DataFrame:
    """Carrega as correções manuais de caixa."""
    return carregar_tabela("correcao_caixa", caminho_banco)

def carregar_fechamento_caixa(caminho_banco: str) -> pd.DataFrame:
    """Carrega os registros de fechamento de caixa."""
    return carregar_tabela("fechamento_caixa", caminho_banco)

def carregar_compras(caminho_banco: str) -> pd.DataFrame:
    """Carrega os registros da tabela de compras."""
    return carregar_tabela("compras", caminho_banco)

def carregar_contas_a_pagar(caminho_banco: str) -> pd.DataFrame:
    """Carrega as contas a pagar."""
    return carregar_tabela("contas_a_pagar", caminho_banco)

def carregar_cartoes_credito(caminho_banco: str) -> pd.DataFrame:
    """Carrega os cartões de crédito cadastrados."""
    return carregar_tabela("cartoes_credito", caminho_banco)

def carregar_saldos_bancos(caminho_banco: str) -> pd.DataFrame:
    """Carrega os saldos dos bancos."""
    return carregar_tabela("saldos_bancos", caminho_banco)

def carregar_metas(caminho_banco: str) -> pd.DataFrame:
    """Carrega as metas cadastradas."""
    return carregar_tabela("metas", caminho_banco)

def carregar_fatura_cartao(caminho_banco: str) -> pd.DataFrame:
    """Carrega as faturas de cartões de crédito."""
    return carregar_tabela("fatura_cartao", caminho_banco)

def carregar_saidas(caminho_banco: str) -> pd.DataFrame:
    """Carrega os lançamentos de saída."""
    return carregar_tabela("saida", caminho_banco)

def carregar_saldos_caixa(caminho_banco: str) -> pd.DataFrame:
    """Carrega os saldos de caixa (caixa e caixa 2)."""
    return carregar_tabela("saldos_caixas", caminho_banco)

def carregar_emprestimos_financiamentos(caminho_banco: str) -> pd.DataFrame:
    """Carrega empréstimos e financiamentos."""
    return carregar_tabela("emprestimos_financiamentos", caminho_banco)

def carregar_taxas_maquinas(caminho_banco: str) -> pd.DataFrame:
    """Carrega as taxas das máquinas de cartão."""
    return carregar_tabela("taxas_maquinas", caminho_banco)

def carregar_entradas(caminho_banco: str) -> pd.DataFrame:
    """Carrega os lançamentos de entrada."""
    return carregar_tabela("entrada", caminho_banco)