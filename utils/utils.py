
import pandas as pd
import hashlib
import re
import sqlite3
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

# Soma os valores de colunas em saldos de caixas
def garantir_trigger_totais_saldos_caixas(caminho_banco: str) -> None:
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