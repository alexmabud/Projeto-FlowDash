"""
Pacote utils
============

Reexporta utilitários comuns do FlowDash para facilitar imports.
"""

from .utils import (
    gerar_hash_senha,
    formatar_moeda,
    formatar_percentual,
    formatar_valor,
    garantir_trigger_totais_saldos_caixas,
    formatar_preco,
    formatar_porcentagem,
    limpar_valor_formatado,   # <-- adicionado
    desformatar_moeda,        # <-- adicionado (alias)
)

__all__ = [
    "gerar_hash_senha",
    "formatar_moeda",
    "formatar_percentual",
    "formatar_valor",
    "garantir_trigger_totais_saldos_caixas",
    "formatar_preco",
    "formatar_porcentagem",
    "limpar_valor_formatado",
    "desformatar_moeda",
]

