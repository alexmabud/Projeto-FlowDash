# utils.py

import pandas as pd
import hashlib
import re
from datetime import datetime, timedelta
from workalendar.america import BrazilDistritoFederal

# =============================
# Funções Auxiliares Gerais
# =============================

# Formata um valor float no padrão monetário brasileiro.Ex: 1234.56 → 'R$ 1.234,56'
def formatar_valor(valor: float) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


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


# Retorna o último dia útil anterior à data fornecida,considerando feriados do Distrito Federal.
def ultimo_dia_util(data: datetime) -> datetime:  
    cal = BrazilDistritoFederal()
    data = pd.to_datetime(data)
    while not cal.is_working_day(data.date()):
        data -= timedelta(days=1)
    return data


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