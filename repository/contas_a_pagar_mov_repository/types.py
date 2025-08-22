"""
Tipos e helpers neutros usados entre os mixins.
"""

from typing import Literal
from decimal import Decimal, ROUND_HALF_UP

TipoObrigacao = Literal["BOLETO", "FATURA_CARTAO", "EMPRESTIMO", "OUTRO"]
ALLOWED_TIPOS = {"BOLETO", "FATURA_CARTAO", "EMPRESTIMO", "OUTRO"}
ALLOWED_CATEGORIAS = {"LANCAMENTO", "PAGAMENTO", "JUROS", "MULTA", "DESCONTO", "AJUSTE", "CANCELAMENTO"}

def _q2(x) -> Decimal:
    """Arredonda em 2 casas decimais (modo financeiro)."""
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
