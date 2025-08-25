"""
Módulo IDs (Shared)
===================

Normalizadores e geradores de UIDs determinísticos para eventos do FlowDash.

Funcionalidades principais
--------------------------
- Normalização de valores numéricos e datas.
- Sanitização de textos (trim, collapse spaces, uppercase).
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
- Usa normalizadores auxiliares:
  - `_fmt_float`: floats com 6 casas decimais
  - `_fmt_date`: datas no padrão `YYYY-MM-DD`
  - `_int_parcelas`: garante mínimo de 1
- Sanitização:
  - `sanitize`: trim simples
  - `sanitize_plus`: trim + collapse + uppercase opcional

Dependências
------------
- hashlib
- typing (Optional, Any)
- datetime (date, datetime)
"""

import hashlib
from typing import Optional, Any
from datetime import date, datetime

# =============== Normalizadores internos ===============

def _fmt_float(v: Optional[float]) -> str:
    """Formata um float com 6 casas decimais, tratando None/erros como 0.0."""
    try:
        return f"{float(v):.6f}"
    except Exception:
        return f"{0.0:.6f}"


def _try_parse_yyyy_mm_dd(s: str) -> Optional[str]:
    """Tenta interpretar string de data e devolver no formato `YYYY-MM-DD`."""
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


def _fmt_date(d: Any) -> str:
    """Normaliza datas para `YYYY-MM-DD`, aceitando date/datetime ou str."""
    if isinstance(d, (date, datetime)):
        return d.strftime("%Y-%m-%d")
    s = str(d or "").strip()
    parsed = _try_parse_yyyy_mm_dd(s)
    return parsed if parsed is not None else s


def _int_parcelas(p: Any) -> int:
    """Garante um inteiro de parcelas (mínimo 1)."""
    try:
        return max(1, int(p or 1))
    except Exception:
        return 1


# =============== Helpers públicos ===============

def sanitize(s: Optional[str]) -> str:
    """Trim simples (string vazia se None)."""
    return (s or "").strip()


def sanitize_plus(s: Optional[str], upper: bool = False) -> str:
    """Normaliza espaços internos e, opcionalmente, aplica maiúsculas."""
    base = " ".join((s or "").strip().split())
    return base.upper() if upper else base


def hash_uid(*parts) -> str:
    """Gera um SHA-256 determinístico a partir das partes concatenadas por '|'. """
    base = "|".join(str(p) for p in parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


# =============== Construtores semânticos de UID ===============

def uid_venda_liquidacao(*args, **kwargs):
    """
    UID para liquidação de venda.

    Compatível com:
      - NOVO (10 args):
        (data_venda, data_liq, valor_bruto, forma, parcelas, bandeira, maquineta, banco_destino, taxa_percentual, usuario)
      - LEGADO (8 args):
        (data_liq, valor_liq, forma, maquineta, bandeira, parcelas, banco, usuario)

    Retorna um SHA-256 determinístico.
    """
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
    # permite chamar nomeando campos (qualquer subset, usa defaults)
    data_venda       = kwargs.get("data_venda", "")
    data_liq         = kwargs.get("data_liq", "")
    valor_bruto      = kwargs.get("valor_bruto", 0.0)
    forma            = kwargs.get("forma", "")
    parcelas         = kwargs.get("parcelas", 1)
    bandeira         = kwargs.get("bandeira", "")
    maquineta        = kwargs.get("maquineta", "")
    banco_destino    = kwargs.get("banco_destino", "")
    taxa_percentual  = kwargs.get("taxa_percentual", 0.0)
    usuario          = kwargs.get("usuario", "")

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


def uid_saida_dinheiro(data, valor, origem, categoria, sub, desc, usuario):
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


def uid_saida_bancaria(data, valor, banco, forma, categoria, sub, desc, usuario):
    """UID para saída bancária (PIX/DÉBITO/etc.)."""
    return hash_uid(
        "BANCARIA",
        sanitize_plus(forma, upper=True),
        _fmt_date(data),
        _fmt_float(valor),
        sanitize_plus(banco, upper=True),
        sanitize_plus(categoria, upper=True),
        sanitize_plus(sub, upper=True),
        sanitize_plus(desc, upper=False),
        sanitize_plus(usuario, upper=True),
    )


def uid_credito_programado(data, valor, parcelas, cartao, categoria, sub, desc, usuario):
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


def uid_boleto_programado(data, valor, parcelas, venc1, categoria, sub, desc, usuario):
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


def uid_correcao_caixa(data, banco, valor, obs, ajuste_id):
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
