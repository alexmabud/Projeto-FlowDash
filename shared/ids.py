import hashlib
from typing import Optional, Any
from datetime import date, datetime

# =============== Normalizadores base ===============

def _fmt_float(v: Optional[float]) -> str:
    """Formata float com 6 casas. Trata None defensivamente como 0.0."""
    try:
        return f"{float(v):.6f}"
    except Exception:
        return f"{0.0:.6f}"

def _try_parse_yyyy_mm_dd(s: str) -> Optional[str]:
    """Tenta interpretar strings comuns e devolver YYYY-MM-DD."""
    s = (s or "").strip()
    if not s:
        return None
    # Já no padrão?
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    #  DD/MM/YYYY
    try:
        dt = datetime.strptime(s, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    #  YYYY/MM/DD
    try:
        dt = datetime.strptime(s, "%Y/%m/%d")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    return None

def _fmt_date(d: Any) -> str:
    """Normaliza datas para YYYY-MM-DD (aceita date, datetime ou string comum)."""
    if isinstance(d, (date, datetime)):
        return d.strftime("%Y-%m-%d")
    s = str(d or "").strip()
    parsed = _try_parse_yyyy_mm_dd(s)
    return parsed if parsed is not None else s

def _int_parcelas(p: Any) -> int:
    """Garante inteiro >= 1 para parcelas."""
    try:
        return max(1, int(p or 1))
    except Exception:
        return 1

def sanitize(s: Optional[str]) -> str:
    """Trim simples (compat)."""
    return (s or "").strip()

def sanitize_plus(s: Optional[str], upper: bool = False) -> str:
    """
    Trim + colapso de espaços internos.
    Opcionalmente UPPER (útil para chaves lógicas).
    """
    base = " ".join((s or "").strip().split())
    return base.upper() if upper else base

def hash_uid(*parts) -> str:
    """
    Gera um SHA-256 determinístico a partir das partes recebidas.
    Usa '|' como separador.
    """
    base = "|".join(str(p) for p in parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

# ====== Construtores semânticos de trans_uid ======

def uid_venda_liquidacao(data_liq, valor_liq, forma, maquineta, bandeira, parcelas, banco, usuario):
    return hash_uid(
        "VENDA_LIQ",
        _fmt_date(data_liq),
        _fmt_float(valor_liq),
        sanitize_plus(forma, upper=True),
        sanitize_plus(maquineta, upper=True),
        sanitize_plus(bandeira, upper=True),
        _int_parcelas(parcelas),
        sanitize_plus(banco, upper=True),
        sanitize_plus(usuario, upper=True),
    )

def uid_saida_dinheiro(data, valor, origem, categoria, sub, desc, usuario):
    return hash_uid(
        "DINHEIRO",
        _fmt_date(data),
        _fmt_float(valor),
        sanitize_plus(origem, upper=True),
        sanitize_plus(categoria, upper=True),
        sanitize_plus(sub, upper=True),
        sanitize_plus(desc, upper=False),       # texto livre
        sanitize_plus(usuario, upper=True),
    )

def uid_saida_bancaria(data, valor, banco, forma, categoria, sub, desc, usuario):
    return hash_uid(
        "BANCARIA",
        sanitize_plus(forma, upper=True),
        _fmt_date(data),
        _fmt_float(valor),
        sanitize_plus(banco, upper=True),
        sanitize_plus(categoria, upper=True),
        sanitize_plus(sub, upper=True),
        sanitize_plus(desc, upper=False),       # texto livre
        sanitize_plus(usuario, upper=True),
    )

def uid_credito_programado(data, valor, parcelas, cartao, categoria, sub, desc, usuario):
    return hash_uid(
        "CREDITO",
        sanitize_plus(cartao, upper=True),
        _fmt_date(data),
        _fmt_float(valor),
        _int_parcelas(parcelas),
        sanitize_plus(categoria, upper=True),
        sanitize_plus(sub, upper=True),
        sanitize_plus(desc, upper=False),       # texto livre
        sanitize_plus(usuario, upper=True),
    )

def uid_boleto_programado(data, valor, parcelas, venc1, categoria, sub, desc, usuario):
    return hash_uid(
        "BOLETO",
        _fmt_date(data),
        _fmt_float(valor),
        _int_parcelas(parcelas),
        _fmt_date(venc1),
        sanitize_plus(categoria, upper=True),
        sanitize_plus(sub, upper=True),
        sanitize_plus(desc, upper=False),       # texto livre
        sanitize_plus(usuario, upper=True),
    )

def uid_correcao_caixa(data, banco, valor, obs, ajuste_id):
    return hash_uid(
        "CORR_CAIXA",
        _fmt_date(data),
        sanitize_plus(banco, upper=True),
        _fmt_float(valor),
        sanitize_plus(obs, upper=False),        # texto livre
        int(ajuste_id or 0),
    )