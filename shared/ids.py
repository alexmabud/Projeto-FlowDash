"""
Normalizadores e geradores de UIDs determinísticos.

Resumo
------
Funções utilitárias para:
- normalização de valores numéricos e datas,
- sanitização de textos (trim/uppercase),
- composição de identificadores determinísticos (SHA-256) para eventos
  do sistema (venda, saída, crédito, boleto, correções).

Estilo
------
Docstrings padronizadas no estilo Google (pt-BR).
"""

import hashlib
from typing import Optional, Any
from datetime import date, datetime

# =============== Normalizadores base ===============

def _fmt_float(v: Optional[float]) -> str:
    """Formata um float com 6 casas decimais.

    Trata valores inválidos/None defensivamente como ``0.0``.

    Args:
        v (Optional[float]): Valor numérico a formatar.

    Returns:
        str: Representação com 6 casas decimais.
    """
    try:
        return f"{float(v):.6f}"
    except Exception:
        return f"{0.0:.6f}"


def _try_parse_yyyy_mm_dd(s: str) -> Optional[str]:
    """Tenta interpretar uma string de data e devolver no formato ``YYYY-MM-DD``.

    Formatos aceitos:
      - ``YYYY-MM-DD`` (já normalizado)
      - ``DD/MM/YYYY``
      - ``YYYY/MM/DD``

    Args:
        s (str): Data em formato textual.

    Returns:
        Optional[str]: Data normalizada em ``YYYY-MM-DD``; ``None`` se não reconhecer.
    """
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
    """Normaliza datas para ``YYYY-MM-DD``.

    Aceita objetos ``date``/``datetime`` ou strings comuns. Para strings,
    tenta converter via :func:`_try_parse_yyyy_mm_dd`; caso falhe, retorna
    a string original (trimada).

    Args:
        d (Any): Valor representando uma data.

    Returns:
        str: Data em ``YYYY-MM-DD`` ou o texto original quando não reconhecido.
    """
    if isinstance(d, (date, datetime)):
        return d.strftime("%Y-%m-%d")
    s = str(d or "").strip()
    parsed = _try_parse_yyyy_mm_dd(s)
    return parsed if parsed is not None else s


def _int_parcelas(p: Any) -> int:
    """Garante um inteiro de parcelas (mínimo 1).

    Args:
        p (Any): Valor a ser convertido.

    Returns:
        int: Número de parcelas, com mínimo de 1.
    """
    try:
        return max(1, int(p or 1))
    except Exception:
        return 1


def sanitize(s: Optional[str]) -> str:
    """Remove espaços nas extremidades (trim simples).

    Args:
        s (Optional[str]): Texto de entrada.

    Returns:
        str: Texto trimado (string vazia se ``None``).
    """
    return (s or "").strip()


def sanitize_plus(s: Optional[str], upper: bool = False) -> str:
    """Normaliza espaços e, opcionalmente, aplica maiúsculas.

    Opera com:
      - trim nas extremidades,
      - colapso de espaços internos (múltiplos → um),
      - conversão para UPPER quando solicitado.

    Args:
        s (Optional[str]): Texto de entrada.
        upper (bool): Se ``True``, retorna o texto em maiúsculas.

    Returns:
        str: Texto normalizado.
    """
    base = " ".join((s or "").strip().split())
    return base.upper() if upper else base


def hash_uid(*parts) -> str:
    """Gera um hash SHA-256 determinístico a partir de partes textuais.

    As partes são concatenadas com ``'|'`` como separador antes do hash.

    Args:
        *parts: Componentes (qualquer tipo conversível para ``str``).

    Returns:
        str: Hex digest SHA-256 (64 caracteres).
    """
    base = "|".join(str(p) for p in parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

# ====== Construtores semânticos de trans_uid ======

def uid_venda_liquidacao(data_liq, valor_liq, forma, maquineta, bandeira, parcelas, banco, usuario):
    """UID para liquidação de venda.

    Componentes considerados: data de liquidação, valor líquido, forma de pagamento,
    maquineta, bandeira, parcelas, banco e usuário (todos normalizados).

    Args:
        data_liq: Data da liquidação (date/datetime/str).
        valor_liq: Valor líquido.
        forma: Forma de pagamento.
        maquineta: Nome da maquineta/PSP.
        bandeira: Bandeira do cartão (quando aplicável).
        parcelas: Quantidade de parcelas.
        banco: Banco de destino.
        usuario: Usuário responsável.

    Returns:
        str: UID determinístico (SHA-256 hexdigest).
    """
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
    """UID para saída em dinheiro (Caixa/Caixa 2).

    Args:
        data: Data do lançamento.
        valor: Valor da saída.
        origem: Origem do dinheiro (ex.: "Caixa", "Caixa 2").
        categoria: Categoria da saída.
        sub: Subcategoria da saída.
        desc: Descrição livre.
        usuario: Usuário responsável.

    Returns:
        str: UID determinístico (SHA-256 hexdigest).
    """
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
    """UID para saída bancária (PIX/DÉBITO/etc.).

    Args:
        data: Data do lançamento.
        valor: Valor da saída.
        banco: Banco de saída.
        forma: Forma de pagamento (ex.: PIX, DÉBITO).
        categoria: Categoria da saída.
        sub: Subcategoria da saída.
        desc: Descrição livre.
        usuario: Usuário responsável.

    Returns:
        str: UID determinístico (SHA-256 hexdigest).
    """
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
    """UID para despesa a crédito (parcelada) programada em faturas.

    Args:
        data: Data da compra.
        valor: Valor total da compra.
        parcelas: Quantidade de parcelas.
        cartao: Nome do cartão.
        categoria: Categoria da despesa.
        sub: Subcategoria da despesa.
        desc: Descrição livre.
        usuario: Usuário responsável.

    Returns:
        str: UID determinístico (SHA-256 hexdigest).
    """
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
    """UID para boleto programado (com/sem parcelas).

    Args:
        data: Data de criação/programação.
        valor: Valor total.
        parcelas: Quantidade de parcelas.
        venc1: Vencimento da primeira parcela.
        categoria: Categoria do lançamento.
        sub: Subcategoria do lançamento.
        desc: Descrição livre.
        usuario: Usuário responsável.

    Returns:
        str: UID determinístico (SHA-256 hexdigest).
    """
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
    """UID para lançamentos de correção/ajuste de caixa/banco.

    Args:
        data: Data do ajuste.
        banco: Banco/caixa ajustado.
        valor: Valor do ajuste.
        obs: Observação/descrição livre.
        ajuste_id: Identificador numérico do ajuste (ou equivalente).

    Returns:
        str: UID determinístico (SHA-256 hexdigest).
    """
    return hash_uid(
        "CORR_CAIXA",
        _fmt_date(data),
        sanitize_plus(banco, upper=True),
        _fmt_float(valor),
        sanitize_plus(obs, upper=False),        # texto livre
        int(ajuste_id or 0),
    )