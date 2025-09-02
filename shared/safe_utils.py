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

# -------------------- len seguro e checagens --------------------

def len_safe(x: Any) -> int:
    """len robusto (para int/float/None vira len(str(x)) ou 0)."""
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
    """Vazio para UI: None -> True; ints/floats nunca são 'em branco'."""
    if x is None:
        return True
    if isinstance(x, (int, float)):
        return False
    return str(x).strip() == ""

# -------------------- parses básicos --------------------

def _parse_money(v: Any) -> float:
    """Aceita '400,00', '1.234,56', 400 (int/float)."""
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def _parse_date_any(s: Any) -> str:
    """Normaliza 'YYYY-MM-DD'; aceita 'DD/MM/YYYY', 'YYYY/MM/DD', etc."""
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
    """Parcelas como inteiro >=1."""
    try:
        return max(1, int(p or 1))
    except Exception:
        return 1

def _norm_str(x: Any) -> Optional[str]:
    """String normalizada (None se vazio)."""
    if x is None:
        return None
    s = str(x).strip()
    return s or None

# -------------------- normalizador principal do form --------------------

def coerce_saida_form(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza um dict do formulário "Lançar Saída" e deriva campos úteis:
    - valor (float), parcelas (int>=1), data_norm (YYYY-MM-DD)
    - forma_norm em {'DINHEIRO','PIX','DÉBITO'} com heurísticas
    - strings seguras em texto
    """
    out: Dict[str, Any] = dict(d)

    out["valor"] = _parse_money(d.get("valor"))
    out["parcelas"] = _int_ge1(d.get("parcelas"))
    out["data_norm"] = _parse_date_any(d.get("data_evento") or d.get("data") or d.get("data_compra"))

    # Texto seguro
    for k in ("categoria", "subcategoria", "descricao", "origem", "banco", "usuario", "trans_uid", "credor"):
        out[k] = _norm_str(d.get(k))

    # Forma normalizada (considera aliases)
    raw_forma = (
        d.get("forma")
        or d.get("forma_pagamento")
        or d.get("metodo")
        or d.get("meio_pagamento")
    )
    forma_up = (str(raw_forma).strip().upper()) if raw_forma is not None else ""

    if forma_up == "DEBITO":
        forma_up = "DÉBITO"

    # Heurística:
    # - "BOLETO" do UI vira DINHEIRO se origem for Caixa/Caixa 2; senão tenta PIX
    # - se forma vazia: Caixa/Caixa 2 => DINHEIRO; banco informado => PIX; fallback DINHEIRO
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
    """
    Normaliza o payload do formulário e grava a saída:
    - Faz parse seguro (datas, valores, parcelas) e resolve forma de pagamento (BOLETO -> DINHEIRO/PIX conforme origem/banco).
    - Evita qualquer uso de len(...) em int/float (usa len_safe se precisar).
    - Chama LedgerService.registrar_lancamento para saída avulsa (sem obrigacao_id).

    Retorna:
        (True, resultado)  em caso de sucesso
        (False, mensagem)  em caso de erro amigável
    """
    try:
        d = coerce_saida_form(payload)

        # Validações mínimas (sem usar len() direto):
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
        "valor": "400,00",
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
