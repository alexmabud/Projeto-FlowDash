# shared/ids.py
"""
Módulo IDs (Shared)
===================

Normalizadores e geradores de UIDs determinísticos para eventos do FlowDash.

Funcionalidades principais
--------------------------
- Normalização de valores numéricos e datas.
- Sanitização de textos (trim, remoção de controles, normalização de espaços).
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
  - `_fmt_float`: floats com 6 casas decimais
  - `_fmt_date`: datas no padrão `YYYY-MM-DD` (com tolerância a formatos comuns)
  - `_int_parcelas`: garante mínimo de 1
- Sanitização robusta:
  - `sanitize`: aceita qualquer tipo (int/float/None), converte para str antes de operar
  - `sanitize_plus`: idem, com colapso de espaços internos e maiúsculas opcionais
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Optional
from datetime import date, datetime

# =============== Normalizadores internos ===============

_CTRL_RE = re.compile(r"[\x00-\x1F\x7F]")  # remove caracteres de controle


def _to_str(x: Any) -> str:
    """Converte para string segura (None -> '') e remove caracteres de controle."""
    if x is None:
        return ""
    try:
        s = str(x)
    except Exception:
        return ""
    return _CTRL_RE.sub("", s)


def _fmt_float(v: Any) -> str:
    """Formata um float com 6 casas decimais, tratando None/erros como 0.0.

    Args:
        v: Valor numérico ou representável como número.

    Returns:
        str: Valor formatado com 6 casas decimais.
    """
    try:
        return f"{float(v):.6f}"
    except Exception:
        return f"{0.0:.6f}"


def _try_parse_yyyy_mm_dd(s: str) -> Optional[str]:
    """Tenta interpretar string de data e devolver no formato `YYYY-MM-DD`.

    Aceita formatos comuns como:
      - YYYY-MM-DD
      - DD/MM/YYYY
      - YYYY/MM/DD
      - DD-MM-YYYY
      - ISO-like com horário (será truncado para a data)

    Args:
        s: String de data.

    Returns:
        str | None: Data normalizada (YYYY-MM-DD) ou None se não reconhecida.
    """
    s = _to_str(s).strip()
    if not s:
        return None

    # Tentativas explícitas de parsing (ordem de preferência)
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue

    # Fallback: fromisoformat (aceita 'YYYY-MM-DD' e variações com tempo)
    try:
        return datetime.fromisoformat(s).date().strftime("%Y-%m-%d")
    except Exception:
        return None


def _fmt_date(d: Any) -> str:
    """Normaliza datas para `YYYY-MM-DD`, aceitando date/datetime/str.

    Args:
        d: Valor representando uma data.

    Returns:
        str: Data normalizada (YYYY-MM-DD) ou a string sanitizada original se não reconhecida.
    """
    if isinstance(d, (date, datetime)):
        return d.strftime("%Y-%m-%d")
    parsed = _try_parse_yyyy_mm_dd(_to_str(d))
    return parsed if parsed is not None else _to_str(d).strip()


def _int_parcelas(p: Any) -> int:
    """Garante um inteiro de parcelas (mínimo 1).

    Args:
        p: Valor numérico de parcelas.

    Returns:
        int: Número de parcelas, com mínimo de 1.
    """
    try:
        return max(1, int(p or 1))
    except Exception:
        return 1


# =============== Helpers públicos ===============

def sanitize(s: Any) -> str:
    """Trim simples robusto (aceita qualquer tipo).

    Args:
        s: Valor a sanitizar.

    Returns:
        str: String sem espaços nas pontas e sem caracteres de controle.
    """
    return _to_str(s).strip()


def sanitize_plus(s: Any, upper: bool = False) -> str:
    """Normaliza espaços internos e, opcionalmente, aplica maiúsculas.

    Args:
        s: Valor a sanitizar.
        upper: Se True, retorna a string em maiúsculas.

    Returns:
        str: String sanitizada (com espaços colapsados) e opcionalmente em maiúsculas.
    """
    base = " ".join(_to_str(s).strip().split())
    return base.upper() if upper else base


def hash_uid(*parts: Any) -> str:
    """Gera um SHA-256 determinístico a partir das partes concatenadas por '|'.

    Args:
        *parts: Partes que compõem o identificador.

    Returns:
        str: Hash SHA-256 (64 caracteres) das partes normalizadas.
    """
    base = "|".join(_to_str(p) for p in parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


# =============== Construtores semânticos de UID ===============

def uid_venda_liquidacao(*args: Any, **kwargs: Any) -> str:
    """UID para liquidação de venda.

    Compatível com:
      - NOVO (10 args):
        (data_venda, data_liq, valor_bruto, forma, parcelas, bandeira,
         maquineta, banco_destino, taxa_percentual, usuario)
      - LEGADO (8 args):
        (data_liq, valor_liq, forma, maquineta, bandeira, parcelas, banco, usuario)
      - Via kwargs (flexível), com as mesmas chaves do modo NOVO.

    Returns:
        str: Hash determinístico representando a liquidação.
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
    """UID para saída em dinheiro (Caixa/Caixa 2).

    Args:
        data: Data do evento.
        valor: Valor do evento.
        origem: "Caixa" ou "Caixa 2".
        categoria: Categoria da saída.
        sub: Subcategoria da saída.
        desc: Descrição.
        usuario: Usuário operador.

    Returns:
        str: Hash determinístico.
    """
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
    """UID para saída bancária (PIX/DÉBITO/etc.).

    Args:
        data: Data do evento.
        valor: Valor do evento.
        banco: Nome do banco (ou conta).
        forma: Forma de pagamento (PIX/DÉBITO/etc.). "DEBITO" é normalizado para "DÉBITO".
        categoria: Categoria da saída.
        sub: Subcategoria da saída.
        desc: Descrição.
        usuario: Usuário operador.

    Returns:
        str: Hash determinístico.
    """
    forma_up = sanitize_plus(forma, upper=True)
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
    """UID para despesa a crédito (parcelada) programada em faturas.

    Args:
        data: Data do evento.
        valor: Valor do evento.
        parcelas: Número de parcelas (mínimo 1).
        cartao: Nome do cartão/conta do cartão.
        categoria: Categoria da saída.
        sub: Subcategoria da saída.
        desc: Descrição.
        usuario: Usuário operador.

    Returns:
        str: Hash determinístico.
    """
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
    """UID para boleto programado (com/sem parcelas).

    Args:
        data: Data de programação.
        valor: Valor do boleto.
        parcelas: Número de parcelas (mínimo 1).
        venc1: Data do primeiro vencimento.
        categoria: Categoria da saída.
        sub: Subcategoria da saída.
        desc: Descrição.
        usuario: Usuário operador.

    Returns:
        str: Hash determinístico.
    """
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
    """UID para lançamentos de correção/ajuste de caixa/banco.

    Args:
        data: Data do ajuste.
        banco: Nome do banco/caixa alvo do ajuste.
        valor: Valor do ajuste.
        obs: Observação descritiva.
        ajuste_id: Identificador interno do ajuste (inteiro).

    Returns:
        str: Hash determinístico.
    """
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
