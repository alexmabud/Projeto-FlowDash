# shared/ids.py
"""
Módulo IDs (Shared)
===================

Normalizadores e geradores de UIDs determinísticos para eventos do FlowDash.

Funcionalidades principais
--------------------------
- Normalização de valores numéricos e datas.
- Sanitização de textos (trim, remoção de controles, normalização Unicode).
- Geração de identificadores determinísticos (`trans_uid`) usando SHA-256.
- Construtores semânticos de UIDs para diferentes contextos:
  - Venda (liquidação)
  - Saída (dinheiro, bancária)
  - Crédito programado
  - Boleto programado
  - Correção/Ajuste de caixa

Detalhes técnicos
-----------------
- Baseado em `hashlib.sha256` (64 caracteres).
- Normalizadores auxiliares:
  - `_fmt_float`: floats com 6 casas decimais (tolerante a "R$ 1.234,56", "1.234,56", "1234.56")
  - `_fmt_date`: datas no padrão `YYYY-MM-DD` (tolerante a formatos comuns)
  - `_int_parcelas`: garante mínimo de 1
- Sanitização robusta:
  - `sanitize`: aceita qualquer tipo (int/float/None), converte para str antes de operar
  - `sanitize_plus`: idem, com colapso de espaços internos e maiúsculas opcionais
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any, Optional
from datetime import date, datetime

# =============== Normalizadores internos ===============

_CTRL_RE = re.compile(r"[\x00-\x1F\x7F]")  # remove caracteres de controle
_ONLY_NUMERIC_PUNCT_RE = re.compile(r"[^0-9,.\-]")  # mantém apenas dígitos e , . -


def _to_str(x: Any) -> str:
    """Converte para string segura (None -> ''), normaliza Unicode e remove controles."""
    if x is None:
        return ""
    try:
        s = str(x)
    except Exception:
        return ""
    # Normaliza Unicode (ex.: "É" pré/composto) e remove controles
    s = unicodedata.normalize("NFKC", s)
    return _CTRL_RE.sub("", s)


def _to_float(v: Any) -> float:
    """
    Converte para float aceitando padrões BR/US e com símbolos:
    Exemplos aceitos: 'R$ 1.234,56', '1.234,56', '1234.56', '1 234,56', '-123'
    Heurística: o último separador entre ',' e '.' define o separador decimal.
    """
    if isinstance(v, (int, float)):
        return float(v)

    s = _to_str(v).strip()
    if not s:
        return 0.0

    # remove tudo que não for dígito, vírgula, ponto ou sinal
    s = _ONLY_NUMERIC_PUNCT_RE.sub("", s)

    if not s or s in {",", ".", "-", "-.", "-,", "+", "+.", "+,"}:
        return 0.0

    try:
        dot = s.rfind(".")
        comma = s.rfind(",")
        if dot == -1 and comma == -1:
            return float(s)
        if dot > comma:
            # padrão US ('.' decimal, ',' milhar)
            return float(s.replace(",", ""))
        else:
            # padrão BR (',' decimal, '.' milhar)
            return float(s.replace(".", "").replace(",", "."))
    except Exception:
        # fallback simples
        try:
            return float(s.replace(",", "."))
        except Exception:
            return 0.0


def _fmt_float(v: Any) -> str:
    """Formata um float com 6 casas decimais, tolerante a strings BR/US e com símbolos."""
    return f"{_to_float(v):.6f}"


def _try_parse_yyyy_mm_dd(s: str) -> Optional[str]:
    """Interpreta string de data e devolve `YYYY-MM-DD` quando possível."""
    s = _to_str(s).strip()
    if not s:
        return None

    # Suporte a 'YYYYMMDD'
    if re.fullmatch(r"\d{8}", s):
        try:
            return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
        except Exception:
            pass

    # Tentativas explícitas (ordem importa)
    for fmt in (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue

    # Fallback ISO-like (aceita 'YYYY-MM-DDTHH:MM:SS' e variações)
    try:
        return datetime.fromisoformat(s).date().strftime("%Y-%m-%d")
    except Exception:
        return None


def _fmt_date(d: Any) -> str:
    """
    Normaliza datas para `YYYY-MM-DD`, aceitando date/datetime/str.
    Caso não seja possível interpretar, retorna string vazia (determinístico).
    """
    if isinstance(d, (date, datetime)):
        return d.strftime("%Y-%m-%d")
    parsed = _try_parse_yyyy_mm_dd(_to_str(d))
    return parsed if parsed is not None else ""


def _int_parcelas(p: Any) -> int:
    """Garante um inteiro de parcelas (mínimo 1)."""
    try:
        return max(1, int(p or 1))
    except Exception:
        return 1


# =============== Helpers públicos ===============

def sanitize(s: Any) -> str:
    """Trim simples robusto (aceita qualquer tipo) + normalização Unicode."""
    return _to_str(s).strip()


def sanitize_plus(s: Any, upper: bool = False) -> str:
    """Normaliza espaços internos e, opcionalmente, aplica maiúsculas."""
    base = " ".join(_to_str(s).strip().split())
    return base.upper() if upper else base


def hash_uid(*parts: Any) -> str:
    """Gera um SHA-256 determinístico a partir das partes concatenadas por '|'."""
    base = "|".join(_to_str(p) for p in parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


# =============== Construtores semânticos de UID ===============

def uid_venda_liquidacao(*args: Any, **kwargs: Any) -> str:
    """UID para liquidação de venda (suporta modo NOVO, LEGADO e kwargs)."""
    # ----------- NOVO: 10 argumentos -----------
    if len(args) == 10 and not kwargs:
        (
            data_venda,
            data_liq,
            valor_bruto,
            forma,
            parcelas,
            bandeira,
            maquineta,
            banco_destino,
            taxa_percentual,
            usuario,
        ) = args

        return hash_uid(
            "VENDA_LIQ_V2",
            _fmt_date(data_venda),
            _fmt_date(data_liq),
            _fmt_float(valor_bruto),
            sanitize_plus(forma, upper=True),
            _int_parcelas(parcelas),
            sanitize_plus(bandeira, upper=True),
            sanitize_plus(maquineta, upper=True),
            sanitize_plus(banco_destino, upper=True),
            _fmt_float(taxa_percentual),
            sanitize_plus(usuario, upper=True),
        )

    # ----------- LEGADO: 8 argumentos -----------
    if len(args) == 8 and not kwargs:
        (
            data_liq,
            valor_liq,
            forma,
            maquineta,
            bandeira,
            parcelas,
            banco,
            usuario,
        ) = args

        return hash_uid(
            "VENDA_LIQ_V1",
            _fmt_date(data_liq),
            _fmt_float(valor_liq),
            sanitize_plus(forma, upper=True),
            sanitize_plus(maquineta, upper=True),
            sanitize_plus(bandeira, upper=True),
            _int_parcelas(parcelas),
            sanitize_plus(banco, upper=True),
            sanitize_plus(usuario, upper=True),
        )

    # ----------- Suporte via kwargs (flexível) -----------
    data_venda = kwargs.get("data_venda", "")
    data_liq = kwargs.get("data_liq", "")
    valor_bruto = kwargs.get("valor_bruto", 0.0)
    forma = kwargs.get("forma", "")
    parcelas = kwargs.get("parcelas", 1)
    bandeira = kwargs.get("bandeira", "")
    maquineta = kwargs.get("maquineta", "")
    banco_destino = kwargs.get("banco_destino", "")
    taxa_percentual = kwargs.get("taxa_percentual", 0.0)
    usuario = kwargs.get("usuario", "")

    return hash_uid(
        "VENDA_LIQ_V2K",
        _fmt_date(data_venda),
        _fmt_date(data_liq),
        _fmt_float(valor_bruto),
        sanitize_plus(forma, upper=True),
        _int_parcelas(parcelas),
        sanitize_plus(bandeira, upper=True),
        sanitize_plus(maquineta, upper=True),
        sanitize_plus(banco_destino, upper=True),
        _fmt_float(taxa_percentual),
        sanitize_plus(usuario, upper=True),
    )


def uid_saida_dinheiro(
    data: Any, valor: Any, origem: Any, categoria: Any, sub: Any, desc: Any, usuario: Any
) -> str:
    """UID para saída em dinheiro (Caixa/Caixa 2)."""
    return hash_uid(
        "DINHEIRO",
        _fmt_date(data),
        _fmt_float(valor),
        sanitize_plus(origem, upper=True),
        sanitize_plus(categoria, upper=True),
        sanitize_plus(sub, upper=True),
        sanitize_plus(desc, upper=False),
        sanitize_plus(usuario, upper=True),
    )


def uid_saida_bancaria(
    data: Any, valor: Any, banco: Any, forma: Any, categoria: Any, sub: Any, desc: Any, usuario: Any
) -> str:
    """UID para saída bancária (PIX/DÉBITO/etc.)."""
    forma_up = sanitize_plus(forma, upper=True)
    # Normaliza 'DEBITO' -> 'DÉBITO' (coerente com observações e logs)
    if forma_up == "DEBITO":
        forma_up = "DÉBITO"
    return hash_uid(
        "BANCARIA",
        forma_up,
        _fmt_date(data),
        _fmt_float(valor),
        sanitize_plus(banco, upper=True),
        sanitize_plus(categoria, upper=True),
        sanitize_plus(sub, upper=True),
        sanitize_plus(desc, upper=False),
        sanitize_plus(usuario, upper=True),
    )


def uid_credito_programado(
    data: Any, valor: Any, parcelas: Any, cartao: Any, categoria: Any, sub: Any, desc: Any, usuario: Any
) -> str:
    """UID para despesa a crédito (parcelada) programada em faturas."""
    return hash_uid(
        "CREDITO",
        sanitize_plus(cartao, upper=True),
        _fmt_date(data),
        _fmt_float(valor),
        _int_parcelas(parcelas),
        sanitize_plus(categoria, upper=True),
        sanitize_plus(sub, upper=True),
        sanitize_plus(desc, upper=False),
        sanitize_plus(usuario, upper=True),
    )


def uid_boleto_programado(
    data: Any, valor: Any, parcelas: Any, venc1: Any, categoria: Any, sub: Any, desc: Any, usuario: Any
) -> str:
    """UID para boleto programado (com/sem parcelas)."""
    return hash_uid(
        "BOLETO",
        _fmt_date(data),
        _fmt_float(valor),
        _int_parcelas(parcelas),
        _fmt_date(venc1),
        sanitize_plus(categoria, upper=True),
        sanitize_plus(sub, upper=True),
        sanitize_plus(desc, upper=False),
        sanitize_plus(usuario, upper=True),
    )


def uid_correcao_caixa(data: Any, banco: Any, valor: Any, obs: Any, ajuste_id: Any) -> str:
    """UID para lançamentos de correção/ajuste de caixa/banco."""
    return hash_uid(
        "CORR_CAIXA",
        _fmt_date(data),
        sanitize_plus(banco, upper=True),
        _fmt_float(valor),
        sanitize_plus(obs, upper=False),
        int(ajuste_id or 0),
    )


# API pública explícita
__all__ = [
    "sanitize",
    "sanitize_plus",
    "hash_uid",
    "uid_venda_liquidacao",
    "uid_saida_dinheiro",
    "uid_saida_bancaria",
    "uid_credito_programado",
    "uid_boleto_programado",
    "uid_correcao_caixa",
]
