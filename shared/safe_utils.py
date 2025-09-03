# shared/safe_utils.py
"""
Helpers seguros para handlers de formulário (UI) e integração com o Ledger.

O que tem aqui:
- len_safe(x), is_blank(x): utilitários à prova de int/None.
- coerce_saida_form(payload): normaliza dados do form "Lançar Saída"
  (datas, valores, parcelas, forma_norm em {'DINHEIRO','PIX','DÉBITO'}).
- processar_salvar_saida(payload, db_path): chama o LedgerService.registrar_lancamento
  com tudo já normalizado — sem risco de 'len()' em int/float.

Uso típico no handler do botão "Salvar":
----------------------------------------
from shared.safe_utils import processar_salvar_saida

payload = {
    "data_evento": "02/09/2025",
    "valor": "400,00",
    "forma": "BOLETO",        # será mapeado p/ DINHEIRO (se origem=Caixa/Caixa 2) ou PIX (se houver banco)
    "categoria": "Outros",
    "subcategoria": "Outros",
    "descricao": "sss",
    "parcelas": 4,            # será coerced p/ int>=1 (mas saída avulsa grava 1x)
    "origem": "Caixa",        # importante p/ BOLETO -> DINHEIRO
    "banco": None,            # obrigatório se forma final for PIX/DÉBITO
    "usuario": "tester",
    # "trans_uid": "opcional",
}

ok, out = processar_salvar_saida(payload, db_path="./data/flowdash_data.db")
if ok:
    print("Lançamento criado:", out)
else:
    print("Erro:", out)
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
from datetime import datetime

__all__ = [
    "len_safe",
    "is_blank",
    "coerce_saida_form",
    "processar_salvar_saida",
]

# -------------------- len seguro e checagens --------------------


def len_safe(x: Any) -> int:
    """Retorna um 'len' robusto (para int/float/None vira len(str(x)) ou 0)."""
    if x is None:
        return 0
    try:
        return len(x)  # strings/listas/tuplas
    except TypeError:
        try:
            return len(str(x))
        except Exception:
            return 0


def is_blank(x: Any) -> bool:
    """Define 'vazio' para UI: None -> True; ints/floats nunca são 'em branco'."""
    if x is None:
        return True
    if isinstance(x, (int, float)):
        return False
    return str(x).strip() == ""


# -------------------- parses básicos --------------------


def _parse_money(v: Any) -> float:
    """Converte valores monetários em float de forma robusta.

    Aceita:
        - "1.234,56" (pt-BR) → 1234.56
        - "1,234.56" (en-US) → 1234.56
        - "1234,56"         → 1234.56
        - "1234.56"         → 1234.56
        - 1234 / 1234.56    → 1234.0 / 1234.56

    Regras:
        - Quando há ',' e '.': assume que o separador decimal é o que aparece
          mais à direita; o outro é separador de milhar e é removido.
        - Quando há só ',': trata como separador decimal.
        - Quando há só '.': trata como separador decimal.
    """
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)

    s = str(v).strip().replace(" ", "")
    if not s:
        return 0.0

    has_dot = "." in s
    has_comma = "," in s

    try:
        if has_dot and has_comma:
            # separador decimal = o símbolo mais à direita
            last_dot = s.rfind(".")
            last_comma = s.rfind(",")
            if last_comma > last_dot:
                # decimal = ',', milhares = '.'
                s = s.replace(".", "").replace(",", ".")
            else:
                # decimal = '.', milhares = ','
                s = s.replace(",", "")
        elif has_comma:
            # apenas vírgula -> decimal
            s = s.replace(",", ".")
        else:
            # apenas ponto (ou nenhum) -> já está OK
            pass
        return float(s)
    except Exception:
        return 0.0


def _parse_date_any(s: Any) -> str:
    """Normaliza datas para 'YYYY-MM-DD'; aceita 'DD/MM/YYYY', 'YYYY/MM/DD', etc."""
    if not s:
        return datetime.now().strftime("%Y-%m-%d")
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s).date().strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _int_ge1(p: Any) -> int:
    """Coerção de parcelas para inteiro >= 1."""
    try:
        return max(1, int(p or 1))
    except Exception:
        return 1


def _norm_str(x: Any) -> Optional[str]:
    """Normaliza strings (trim); retorna None quando vazio/whitespace."""
    if x is None:
        return None
    s = str(x).strip()
    return s or None


# -------------------- normalizador principal do form --------------------


def coerce_saida_form(d: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza um dict do formulário "Lançar Saída" e deriva campos úteis.

    Gera:
        - valor (float), parcelas (int>=1), data_norm (YYYY-MM-DD)
        - forma_norm em {'DINHEIRO','PIX','DÉBITO'} com heurísticas
        - strings seguras para texto

    Heurística para `forma_norm`:
        - se forma ∈ {'DINHEIRO','PIX','DÉBITO'}: usa direto (normalizando 'DEBITO' -> 'DÉBITO')
        - se forma == 'BOLETO':
            • origem ∈ {'caixa','caixa 2','caixa2'} → 'DINHEIRO'
            • senão, se houver banco → 'PIX'
            • senão → 'DINHEIRO'
        - se forma vazia:
            • origem ∈ {'caixa','caixa 2','caixa2'} → 'DINHEIRO'
            • senão, se houver banco → 'PIX'
            • senão → 'DINHEIRO'
    """
    out: Dict[str, Any] = dict(d)

    out["valor"] = _parse_money(d.get("valor"))
    out["parcelas"] = _int_ge1(d.get("parcelas"))
    out["data_norm"] = _parse_date_any(d.get("data_evento") or d.get("data") or d.get("data_compra"))

    # Texto seguro
    for k in ("categoria", "subcategoria", "descricao", "origem", "banco", "usuario", "trans_uid", "credor"):
        out[k] = _norm_str(d.get(k))

    # Forma normalizada (considera aliases)
    raw_forma = d.get("forma") or d.get("forma_pagamento") or d.get("metodo") or d.get("meio_pagamento")
    forma_up = (str(raw_forma).strip().upper()) if raw_forma is not None else ""

    if forma_up == "DEBITO":
        forma_up = "DÉBITO"

    origem_low = (out.get("origem") or "").lower()
    if forma_up in {"DINHEIRO", "PIX", "DÉBITO"}:
        forma_norm = forma_up
    elif forma_up == "BOLETO":
        forma_norm = "DINHEIRO" if origem_low in {"caixa", "caixa 2", "caixa2"} else ("PIX" if out.get("banco") else "DINHEIRO")
    else:
        if origem_low in {"caixa", "caixa 2", "caixa2"}:
            forma_norm = "DINHEIRO"
        elif out.get("banco"):
            forma_norm = "PIX"
        else:
            forma_norm = "DINHEIRO"

    out["forma_norm"] = forma_norm
    return out


# -------------------- integração direta com o Ledger --------------------


def processar_salvar_saida(payload: Dict[str, Any], db_path: str) -> Tuple[bool, Any]:
    """Normaliza o payload do formulário e grava a saída via Ledger.

    - Faz parse seguro (datas, valores, parcelas) e resolve forma de pagamento (BOLETO -> DINHEIRO/PIX conforme origem/banco).
    - Evita qualquer uso de len(...) em int/float (usa len_safe se precisar).
    - Chama LedgerService.registrar_lancamento para saída avulsa (sem obrigacao_id).

    Retorna:
        (True, resultado) em caso de sucesso
        (False, mensagem) em caso de erro amigável
    """
    try:
        d = coerce_saida_form(payload)

        # Validações mínimas
        if d["valor"] <= 0:
            return (False, "Valor deve ser maior que zero.")

        if d["forma_norm"] in {"PIX", "DÉBITO"} and not d.get("banco"):
            return (False, "Informe o banco para PIX/DÉBITO.")

        # Import tardio evita problemas de import circular em alguns setups
        from services.ledger.service_ledger import LedgerService

        L = LedgerService(db_path)

        resultado = L.registrar_lancamento(
            tipo_evento="SAIDA",
            categoria_evento=d.get("categoria"),
            subcategoria_evento=d.get("subcategoria"),
            valor_evento=d["valor"],
            forma=d["forma_norm"],
            origem=d.get("origem"),
            banco=d.get("banco"),
            descricao=d.get("descricao"),
            usuario=d.get("usuario") or "-",
            trans_uid=d.get("trans_uid"),
            data_evento=d["data_norm"],
        )

        return (True, resultado)

    except Exception as e:
        # Mensagem compacta para UI; para depuração, logue stack no console do servidor
        return (False, f"Falha ao salvar saída: {e}")


# -------------------- teste rápido local --------------------
if __name__ == "__main__":
    sample = {
        "data_evento": "02/09/2025",
        "valor": "1,234.56",
        "forma": "BOLETO",
        "categoria": "Outros",
        "subcategoria": "Outros",
        "descricao": "sss",
        "parcelas": 4,
        "origem": "Caixa",  # importante p/ BOLETO virar DINHEIRO
        "usuario": "tester",
    }
    ok, out = processar_salvar_saida(sample, db_path="./data/flowdash_data.db")
    print("OK?", ok)
    print("OUT:", out)
