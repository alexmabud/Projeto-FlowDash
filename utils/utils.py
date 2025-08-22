"""
Utilitários de dados, formatação e segurança para o FlowDash.

Resumo
------
Coleção de funções auxiliares para:
- cálculos percentuais e datas úteis (considerando feriados do DF),
- formatação e limpeza de valores monetários/percentuais,
- padronização de colunas em DataFrames,
- manutenção de gatilhos de totais em `saldos_caixas`,
- geração e verificação de senhas (hash e força).

Estilo
------
Docstrings padronizadas no estilo Google (pt-BR).
"""

import pandas as pd
import hashlib
import re
import sqlite3
from datetime import datetime, timedelta
from workalendar.america import BrazilDistritoFederal

# =============================
# Funções Auxiliares Gerais
# =============================

def calcular_percentual(valor: float, meta: float) -> float:
    """Calcula o percentual de `valor` em relação à `meta`.

    Trata divisão por zero retornando 0.0 quando `meta` é 0.

    Args:
        valor (float): Valor observado.
        meta (float): Meta de referência.

    Returns:
        float: Percentual arredondado com 2 casas decimais (0.0 se meta == 0).
    """
    if meta == 0:
        return 0.0
    return round((valor / meta) * 100, 2)


def adicionar_dia_semana(df: pd.DataFrame, coluna_data: str = "Data") -> pd.DataFrame:
    """Adiciona a coluna ``Dia_Semana`` a partir de uma coluna de datas.

    Converte a coluna informada para datetime (coercivo) e usa locale ``pt_BR``
    para obter o nome do dia da semana.

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        coluna_data (str): Nome da coluna contendo datas. Padrão: ``"Data"``.

    Returns:
        pd.DataFrame: Cópia do DataFrame com a coluna ``Dia_Semana`` incluída.
    """
    df = df.copy()
    df[coluna_data] = pd.to_datetime(df[coluna_data], errors="coerce")
    df["Dia_Semana"] = df[coluna_data].dt.day_name(locale="pt_BR")
    return df


def ultimo_dia_util(data: datetime) -> datetime:
    """Retorna o último dia útil anterior (ou igual) à data informada.

    Utiliza o calendário de feriados/trabalho do Distrito Federal (workalendar).

    Args:
        data (datetime): Data de referência.

    Returns:
        datetime: Último dia útil não-feriado no DF anterior ou igual a `data`.
    """
    cal = BrazilDistritoFederal()
    data = pd.to_datetime(data)
    while not cal.is_working_day(data.date()):
        data -= timedelta(days=1)
    return data


# === Formatação de Valores =========================================================================================

def formatar_valor(valor: float) -> str:
    """Formata um float como moeda brasileira.

    Exemplos:
        1234.56 -> ``"R$ 1.234,56"``

    Args:
        valor (float): Valor numérico.

    Returns:
        str: Valor formatado em BRL.
    """
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def formatar_percentual(valor: float) -> str:
    """Formata um float como percentual brasileiro (1 casa decimal).

    Exemplos:
        87.5 -> ``"87,5%"``

    Args:
        valor (float): Valor percentual (0–100, tipicamente).

    Returns:
        str: Percentual formatado (vírgula como separador decimal).
    """
    return f"{valor:.1f}%".replace(".", ",")


def formatar_dataframe(df, colunas_monetarias=[], colunas_percentuais=[]):
    """Aplica formatação monetária/percentual em colunas de um DataFrame.

    Observação:
        Usa cópia do DataFrame. Colunas inexistentes são ignoradas.

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        colunas_monetarias (list[str]): Lista de nomes de colunas a formatar como BRL.
        colunas_percentuais (list[str]): Lista de nomes de colunas a formatar como %.

    Returns:
        pd.DataFrame: DataFrame formatado.
    """
    df_formatado = df.copy()
    for col in colunas_monetarias:
        if col in df_formatado.columns:
            df_formatado[col] = df_formatado[col].apply(lambda x: formatar_valor(x) if x is not None else "")
    for col in colunas_percentuais:
        if col in df_formatado.columns:
            df_formatado[col] = df_formatado[col].apply(lambda x: formatar_percentual(x) if x is not None else "")
    return df_formatado


def limpar_valor_formatado(valor_str: str) -> float:
    """Converte uma string monetária brasileira para float.

    Remove ``"R$"``, separadores de milhar e troca vírgula por ponto.

    Exemplos:
        ``"R$ 1.234,56"`` -> ``1234.56``

    Args:
        valor_str (str): Valor formatado como string.

    Returns:
        float: Valor numérico; 0.0 em caso de conversão inválida.
    """
    if isinstance(valor_str, str):
        valor_str = valor_str.replace("R$", "").replace(".", "").replace(",", ".").strip()
        try:
            return float(valor_str)
        except ValueError:
            return 0.0
    return float(valor_str)


def garantir_trigger_totais_saldos_caixas(caminho_banco: str) -> None:
    """(Re)cria triggers para manter totais em `saldos_caixas`.

    Cria dois gatilhos:
      - ``trg_saldos_insert_totais``: após INSERT, atualiza ``caixa_total`` e ``caixa2_total``;
      - ``trg_saldos_update_totais``: após UPDATE das colunas relevantes, recalcula os totais.

    A função detecta automaticamente se a coluna de vendas é ``caixa_vendas`` ou ``caixa_venda``.
    Exige também a existência das colunas: ``caixa``, ``caixa_2``, ``caixa2_dia``, ``caixa_total``,
    ``caixa2_total``.

    Args:
        caminho_banco (str): Caminho para o arquivo SQLite.

    Raises:
        RuntimeError: Se a tabela/colunas necessárias não existirem.
    """
    with sqlite3.connect(caminho_banco) as conn:
        cur = conn.cursor()
        cols = {r[1] for r in cur.execute("PRAGMA table_info(saldos_caixas)").fetchall()}

        # Detecta a coluna de vendas
        if "caixa_vendas" in cols:
            col_vendas = "caixa_vendas"
        elif "caixa_venda" in cols:
            col_vendas = "caixa_venda"
        else:
            raise RuntimeError("Tabela 'saldos_caixas' não possui 'caixa_vendas' nem 'caixa_venda'.")

        # Verifica demais colunas
        required = {"caixa", "caixa_2", "caixa2_dia", "caixa_total", "caixa2_total"}
        missing = required - cols
        if missing:
            raise RuntimeError(f"Faltam colunas em saldos_caixas: {', '.join(sorted(missing))}")

        ddl = f"""
        DROP TRIGGER IF EXISTS trg_saldos_insert_totais;
        DROP TRIGGER IF EXISTS trg_saldos_update_totais;

        -- Atualiza totais após INSERT (qualquer insert)
        CREATE TRIGGER trg_saldos_insert_totais
        AFTER INSERT ON saldos_caixas
        BEGIN
            UPDATE saldos_caixas
            SET 
                caixa_total  = COALESCE(NEW.caixa, 0) + COALESCE(NEW.{col_vendas}, 0),
                caixa2_total = COALESCE(NEW.caixa_2, 0) + COALESCE(NEW.caixa2_dia, 0)
            WHERE rowid = NEW.rowid;
        END;

        -- Atualiza totais após UPDATE das colunas relevantes
        CREATE TRIGGER trg_saldos_update_totais
        AFTER UPDATE OF caixa, {col_vendas}, caixa_2, caixa2_dia ON saldos_caixas
        BEGIN
            UPDATE saldos_caixas
            SET 
                caixa_total  = COALESCE(NEW.caixa, 0) + COALESCE(NEW.{col_vendas}, 0),
                caixa2_total = COALESCE(NEW.caixa_2, 0) + COALESCE(NEW.caixa2_dia, 0)
            WHERE rowid = NEW.rowid;
        END;
        """
        conn.executescript(ddl)
        conn.commit()


# =============================
# Segurança e Autenticação
# =============================

def gerar_hash_senha(senha: str) -> str:
    """Gera o hash SHA-256 de uma senha em texto claro.

    Args:
        senha (str): Senha em texto.

    Returns:
        str: Hash hexadecimal (64 caracteres).
    """
    return hashlib.sha256(senha.encode()).hexdigest()


def senha_forte(senha: str) -> bool:
    """Verifica requisitos mínimos de força da senha.

    A senha é considerada forte se:
      - tiver pelo menos 8 caracteres,
      - contiver letras maiúsculas e minúsculas,
      - contiver números,
      - contiver símbolos (ex.: ``!@#$%^&*(),.?":{}|<>``).

    Args:
        senha (str): Senha a validar.

    Returns:
        bool: ``True`` se atender a todos os requisitos; ``False`` caso contrário.
    """
    if (
        len(senha) >= 8 and
        re.search(r"[A-Z]", senha) and
        re.search(r"[a-z]", senha) and
        re.search(r"[0-9]", senha) and
        re.search(r"[!@#$%^&*(),.?\":{}|<>]", senha)
    ):
        return True
    return False