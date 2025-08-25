"""
Módulo Tipos e Helpers (Contas a Pagar)
=======================================

Este módulo define tipos e funções utilitárias compartilhadas entre os mixins
do repositório `contas_a_pagar_mov_repository`.

Constantes
----------
- `TipoObrigacao`: Literal para tipos de obrigação válidos
  ('BOLETO', 'FATURA_CARTAO', 'EMPRESTIMO', 'OUTRO').
- `ALLOWED_TIPOS`: conjunto com os tipos válidos.
- `ALLOWED_CATEGORIAS`: conjunto com categorias de evento válidas
  ('LANCAMENTO', 'PAGAMENTO', 'JUROS', 'MULTA', 'DESCONTO', 'AJUSTE', 'CANCELAMENTO').

Helpers
-------
- `_q2(x)`: arredonda valores para 2 casas decimais no modo financeiro
  (Decimal + ROUND_HALF_UP).
"""

from typing import Literal
from decimal import Decimal, ROUND_HALF_UP

# ------------------ Tipos e constantes ------------------

TipoObrigacao = Literal["BOLETO", "FATURA_CARTAO", "EMPRESTIMO", "OUTRO"]

ALLOWED_TIPOS = {"BOLETO", "FATURA_CARTAO", "EMPRESTIMO", "OUTRO"}

ALLOWED_CATEGORIAS = {
    "LANCAMENTO",
    "PAGAMENTO",
    "JUROS",
    "MULTA",
    "DESCONTO",
    "AJUSTE",
    "CANCELAMENTO",
}

# ------------------ Helpers ------------------

def _q2(x) -> Decimal:
    """
    Arredonda valor para 2 casas decimais usando `ROUND_HALF_UP`
    (modo financeiro tradicional).

    Parâmetros:
        x: número ou Decimal.

    Retorno:
        Decimal arredondado com 2 casas.
    """
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# API pública explícita
__all__ = ["TipoObrigacao", "ALLOWED_TIPOS", "ALLOWED_CATEGORIAS", "_q2"]
