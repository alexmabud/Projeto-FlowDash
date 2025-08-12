import hashlib
from typing import Optional

def sanitize(s: Optional[str]) -> str:
    return (s or "").strip()

def _fmt_float(v: float) -> str:
    return f"{float(v):.6f}"

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
        str(data_liq),
        _fmt_float(valor_liq),
        sanitize(forma).upper(),
        sanitize(maquineta),
        sanitize(bandeira),
        int(parcelas or 1),
        sanitize(banco),
        sanitize(usuario),
    )

def uid_saida_dinheiro(data, valor, origem, categoria, sub, desc, usuario):
    return hash_uid(
        "DINHEIRO",
        str(data),
        _fmt_float(valor),
        sanitize(origem),
        sanitize(categoria),
        sanitize(sub),
        sanitize(desc),
        sanitize(usuario),
    )

def uid_saida_bancaria(data, valor, banco, forma, categoria, sub, desc, usuario):
    return hash_uid(
        "BANCARIA",
        sanitize(forma).upper(),
        str(data),
        _fmt_float(valor),
        sanitize(banco),
        sanitize(categoria),
        sanitize(sub),
        sanitize(desc),
        sanitize(usuario),
    )

def uid_credito_programado(data, valor, parcelas, cartao, categoria, sub, desc, usuario):
    return hash_uid(
        "CREDITO",
        sanitize(cartao),
        str(data),
        _fmt_float(valor),
        int(parcelas or 1),
        sanitize(categoria),
        sanitize(sub),
        sanitize(desc),
        sanitize(usuario),
    )

def uid_boleto_programado(data, valor, parcelas, venc1, categoria, sub, desc, usuario):
    return hash_uid(
        "BOLETO",
        str(data),
        _fmt_float(valor),
        int(parcelas or 1),
        str(venc1),
        sanitize(categoria),
        sanitize(sub),
        sanitize(desc),
        sanitize(usuario),
    )

def uid_correcao_caixa(data, banco, valor, obs, ajuste_id):
    return hash_uid(
        "CORR_CAIXA",
        str(data),
        sanitize(banco),
        _fmt_float(valor),
        sanitize(obs),
        int(ajuste_id or 0),
    )