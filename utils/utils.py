
import pandas as pd
import hashlib
import re
from datetime import datetime, timedelta
from workalendar.america import BrazilDistritoFederal

# =============================
# Funções Auxiliares Gerais
# =============================

# Calcula o percentual de um valor em relação à meta. Retorna 0 se a meta for zero.
def calcular_percentual(valor: float, meta: float) -> float:    
    if meta == 0:
        return 0.0
    return round((valor / meta) * 100, 2)

# Adiciona uma coluna com o nome do dia da semana, baseado na coluna de datas fornecida.
def adicionar_dia_semana(df: pd.DataFrame, coluna_data: str = "Data") -> pd.DataFrame:
    df = df.copy()
    df[coluna_data] = pd.to_datetime(df[coluna_data], errors="coerce")
    df["Dia_Semana"] = df[coluna_data].dt.day_name(locale="pt_BR")
    return df

# Retorna o último dia útil anterior à data fornecida, considerando feriados do Distrito Federal.
def ultimo_dia_util(data: datetime) -> datetime:  
    cal = BrazilDistritoFederal()
    data = pd.to_datetime(data)
    while not cal.is_working_day(data.date()):
        data -= timedelta(days=1)
    return data

# === Formatação de Valores =========================================================================================

# Formata um valor float como moeda brasileira. Ex: 1234.56 → 'R$ 1.234,56'
def formatar_valor(valor: float) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# Formata um valor float como percentual. Ex: 87.5 → '87,5%'
def formatar_percentual(valor: float) -> str:
    return f"{valor:.1f}%".replace(".", ",")

# Aplica formatação de moeda e percentual a colunas específicas de um DataFrame
def formatar_dataframe(df, colunas_monetarias=[], colunas_percentuais=[]):
    df_formatado = df.copy()
    for col in colunas_monetarias:
        if col in df_formatado.columns:
            df_formatado[col] = df_formatado[col].apply(lambda x: formatar_valor(x) if x is not None else "")
    for col in colunas_percentuais:
        if col in df_formatado.columns:
            df_formatado[col] = df_formatado[col].apply(lambda x: formatar_percentual(x) if x is not None else "")
    return df_formatado

# Limpa a formatação de um valor monetário, convertendo de string para float.
def limpar_valor_formatado(valor_str: str) -> float:
    if isinstance(valor_str, str):
        valor_str = valor_str.replace("R$", "").replace(".", "").replace(",", ".").strip()
        try:
            return float(valor_str)
        except ValueError:
            return 0.0
    return float(valor_str)


# =============================
# Segurança e Autenticação
# =============================

# Gera um hash SHA256 para uma senha.
def gerar_hash_senha(senha: str) -> str:
    return hashlib.sha256(senha.encode()).hexdigest()

# Verifica se a senha é forte: mínimo de 8 caracteres, com maiúscula, minúscula, número e símbolo.
def senha_forte(senha: str) -> bool:
    if (
        len(senha) >= 8 and
        re.search(r"[A-Z]", senha) and
        re.search(r"[a-z]", senha) and
        re.search(r"[0-9]", senha) and
        re.search(r"[!@#$%^&*(),.?\":{}|<>]", senha)
    ):
        return True
    return False