"""
Módulo Utils
============

Funções utilitárias de uso geral no FlowDash.

Inclui:
- Segurança: geração de hash de senha (`gerar_hash_senha`).
- Formatação: moeda BR (`formatar_moeda`), percentual (`formatar_percentual`)
  e wrapper retrocompatível (`formatar_valor`).
- Infra/BD: garantia idempotente da estrutura de `saldos_caixas`
  (`garantir_trigger_totais_saldos_caixas`).

Observações
-----------
- `formatar_moeda` / `formatar_percentual` aceitam int/float/str/Decimal.
- `gerar_hash_senha` usa SHA-256 (sem sal). Para produção, considere sal/pepper.
"""

from __future__ import annotations
import hashlib
from decimal import Decimal, InvalidOperation
from datetime import date, datetime

# -----------------------------------------------------------------------------
# Segurança
# -----------------------------------------------------------------------------
def gerar_hash_senha(senha: str) -> str:
    """
    Gera hash SHA-256 de uma senha em texto claro.

    Parâmetros
    ----------
    senha : str
        Senha informada pelo usuário.

    Retorno
    -------
    str
        Hex digest do hash SHA-256.
    """
    if senha is None:
        senha = ""
    return hashlib.sha256(senha.encode("utf-8")).hexdigest()


# -----------------------------------------------------------------------------
# Formatação
# -----------------------------------------------------------------------------
def _to_decimal(valor) -> Decimal:
    """Converte `valor` para Decimal, retornando 0 em caso de falha."""
    if isinstance(valor, Decimal):
        return valor
    try:
        return Decimal(str(valor))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def formatar_moeda(valor) -> str:
    """
    Formata um valor numérico no padrão BR: `R$ 1.234,56`.
    """
    v = _to_decimal(valor).quantize(Decimal("0.01"))
    s = f"{v:,.2f}"                       # 1,234.56 (en_US)
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")  # 1.234,56
    return f"R$ {s}"


def formatar_percentual(valor, casas: int = 2) -> str:
    """
    Formata um valor como percentual. Ex.: `0.153` -> `15,30%`.

    Regras
    ------
    - Se |valor| <= 1, trata como fração (0.15 -> 15%).
    - Caso contrário, assume que já está em % (15 -> 15%).
    """
    v = _to_decimal(valor)
    if abs(v) <= 1:
        v = v * 100
    fmt = f"{{:,.{casas}f}}".format(v)
    fmt = fmt.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{fmt}%"


def formatar_valor(valor, *, tipo: str = "moeda", casas: int = 2) -> str:
    """
    Wrapper retrocompatível para formatação de valores.

    Parâmetros
    ----------
    valor : Any
        Valor numérico (int, float, str, Decimal).
    tipo : str
        "moeda" (padrão) ou "percentual".
    casas : int
        Casas decimais para percentual.

    Retorno
    -------
    str
        'R$ 1.234,56' (moeda) ou '15,30%' (percentual).
    """
    t = (tipo or "moeda").strip().lower()
    if t in ("percentual", "porcento", "%"):
        return formatar_percentual(valor, casas=casas)
    return formatar_moeda(valor)


# Aliases de retrocompatibilidade (caso páginas usem outros nomes)
formatar_preco = formatar_moeda
formatar_porcentagem = formatar_percentual


# -----------------------------------------------------------------------------
# Infraestrutura / Banco
# -----------------------------------------------------------------------------
def garantir_trigger_totais_saldos_caixas(caminho_banco: str) -> None:
    """
    Garante (idempotente) a estrutura mínima da tabela `saldos_caixas`
    no formato esperado pelo Ledger:

        data TEXT PRIMARY KEY,
        caixa REAL, caixa_2 REAL, caixa_vendas REAL,
        caixa2_dia REAL, caixa_total REAL, caixa2_total REAL

    Observações:
    - Não insere um registro fixo (ex.: id=1). A criação da linha do dia
      é responsabilidade de quem chama (_garantir_linha_saldos_caixas no Ledger).
    - Se a tabela já existir com layout diferente, adiciona as colunas faltantes
      com DEFAULT 0 (idempotente).
    """
    import sqlite3

    colunas_necessarias = {
        "data": "TEXT",
        "caixa": "REAL",
        "caixa_2": "REAL",
        "caixa_vendas": "REAL",
        "caixa2_dia": "REAL",
        "caixa_total": "REAL",
        "caixa2_total": "REAL",
    }

    with sqlite3.connect(caminho_banco, timeout=30) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

        # Cria tabela se não existir (com 'data' como chave natural)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS saldos_caixas (
                data TEXT PRIMARY KEY,
                caixa REAL DEFAULT 0,
                caixa_2 REAL DEFAULT 0,
                caixa_vendas REAL DEFAULT 0,
                caixa2_dia REAL DEFAULT 0,
                caixa_total REAL DEFAULT 0,
                caixa2_total REAL DEFAULT 0
            );
        """)

        # Garante colunas faltantes
        cols = {row[1] for row in conn.execute("PRAGMA table_info('saldos_caixas');").fetchall()}
        for nome, tipo in colunas_necessarias.items():
            if nome not in cols:
                conn.execute(f'ALTER TABLE saldos_caixas ADD COLUMN "{nome}" {tipo} DEFAULT 0;')

        conn.commit()


def limpar_valor_formatado(valor, *, as_decimal: bool = False):
    """
    Converte textos de moeda em número (Decimal ou float).

    Aceita formatos BR e EN:
    - "R$ 1.234,56"  -> 1234.56
    - "1.234,56"     -> 1234.56
    - "1,234.56"     -> 1234.56
    - "2500"         -> 2500.0
    - "- 2.500,00"   -> -2500.00

    Parâmetros
    ----------
    valor : Any
        Texto/num a ser convertido (str, int, float, Decimal).
    as_decimal : bool, opcional
        Se True, retorna `Decimal`; caso contrário, `float`. Padrão: False.

    Retorno
    -------
    Decimal | float
        Número convertido. Valores inválidos retornam 0 (ou Decimal("0")).

    Notas
    -----
    - Mantém compatibilidade com código legado que importava `limpar_valor_formatado`.
    - Heurística de separadores:
        * Se houver '.' e ',', assume padrão BR ('.' milhar, ',' decimal).
        * Se houver só ',', assume vírgula como decimal.
        * Caso contrário, assume ponto como decimal.
    """
    from decimal import Decimal, InvalidOperation
    import re

    # atalho para tipos já numéricos
    if isinstance(valor, (int, float, Decimal)):
        return Decimal(str(valor)) if as_decimal else float(valor)

    if valor is None:
        return Decimal("0") if as_decimal else 0.0

    txt = str(valor).strip()
    if not txt:
        return Decimal("0") if as_decimal else 0.0

    # remove símbolos e letras, preservando dígitos, sinais e separadores
    txt = re.sub(r"[^\d,.\-+]", "", txt)

    # normalização de separadores
    if "," in txt and "." in txt:
        # Ex.: 1.234,56  -> 1234.56
        txt = txt.replace(".", "")
        txt = txt.replace(",", ".")
    elif "," in txt:
        # Ex.: 1234,56 -> 1234.56
        txt = txt.replace(",", ".")

    # múltiplos sinais/sujeiras -> mantém o primeiro sinal se houver
    # e remove sinais sobrando no meio
    txt = re.sub(r"(?<=.)[+\-]", "", txt)

    try:
        dec = Decimal(txt)
    except (InvalidOperation, ValueError):
        dec = Decimal("0")

    return dec if as_decimal else float(dec)


# Alias de retrocompatibilidade
desformatar_moeda = limpar_valor_formatado


def senha_forte(
    senha: str | None,
    *,
    min_len: int = 8,
    requer_maiuscula: bool = True,
    requer_minuscula: bool = True,
    requer_digito: bool = True,
    requer_especial: bool = True,
) -> bool:
    """
    Valida se a senha atende critérios mínimos de força.

    Parâmetros
    ----------
    senha : str | None
        Senha a ser validada.
    min_len : int, opcional
        Tamanho mínimo (padrão: 8).
    requer_maiuscula : bool, opcional
        Exige pelo menos 1 letra maiúscula (padrão: True).
    requer_minuscula : bool, opcional
        Exige pelo menos 1 letra minúscula (padrão: True).
    requer_digito : bool, opcional
        Exige pelo menos 1 dígito (padrão: True).
    requer_especial : bool, opcional
        Exige pelo menos 1 caractere especial (padrão: True).

    Retorno
    -------
    bool
        True se a senha cumpre os critérios; caso contrário, False.

    Observações
    -----------
    - Função retrocompatível com código legado que importava `senha_forte` de `utils.utils`.
    - Para produção, considere políticas adicionais (ex.: verificação contra listas de senhas vazadas).
    """
    if not isinstance(senha, str):
        return False

    if len(senha) < int(min_len):
        return False

    has_upper = any(c.isupper() for c in senha)
    has_lower = any(c.islower() for c in senha)
    has_digit = any(c.isdigit() for c in senha)
    # Considera qualquer caractere que não seja letra nem dígito como especial
    has_special = any(not c.isalnum() for c in senha)

    if requer_maiuscula and not has_upper:
        return False
    if requer_minuscula and not has_lower:
        return False
    if requer_digito and not has_digit:
        return False
    if requer_especial and not has_special:
        return False

    return True



def coerce_data(value=None) -> date:
    """
    Normaliza 'value' para datetime.date.
    Aceita: None, date, 'YYYY-MM-DD', 'DD/MM/YYYY', 'DD-MM-YYYY'.
    Se vier vazio/None, retorna a data de hoje.
    """
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return date.today()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    raise ValueError(f"Data inválida: {value!r}")


# utils/utils.py
from types import SimpleNamespace
import os

def resolve_db_path(obj) -> str:
    """
    Normaliza o 'caminho do banco' aceitando string/Path/objetos de config.
    Retorna sempre uma string com o path.
    Levanta TypeError se não conseguir resolver.
    """
    if obj is None:
        raise TypeError("Caminho do banco não informado.")

    # Já é string ou PathLike
    if isinstance(obj, (str, os.PathLike)):
        return str(obj)

    # SimpleNamespace com atributos comuns
    if isinstance(obj, SimpleNamespace):
        for key in ("db_path", "caminho_banco", "database"):
            if hasattr(obj, key):
                return str(getattr(obj, key))

    # Qualquer objeto com atributo comum
    for key in ("db_path", "caminho_banco", "database"):
        if hasattr(obj, key):
            return str(getattr(obj, key))

    raise TypeError(f"expected str, bytes or os.PathLike object, got {type(obj).__name__}")
