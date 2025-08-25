"""
Pacote `utils`
==============

Reexporta utilitários de uso comum no FlowDash para facilitar imports.

Exemplos
--------
- `from utils import formatar_moeda`
- `from utils import formatar_valor`   # compatível (alias se necessário)
- `from utils import gerar_hash_senha`
"""

# Importa o que certamente existe no módulo utils.utils
from .utils import (
    gerar_hash_senha,
    formatar_percentual,
    garantir_trigger_totais_saldos_caixas,
)

# Tenta expor formatar_valor; se não existir, faz alias para formatar_moeda
try:
    from .utils import formatar_valor  # preferencial
except Exception:
    from .utils import formatar_moeda as formatar_valor  # compatibilidade

# Expõe também formatar_moeda; se não existir, alias para formatar_valor
try:
    from .utils import formatar_moeda
except Exception:
    def formatar_moeda(v):
        return formatar_valor(v)

__all__ = [
    "gerar_hash_senha",
    "formatar_moeda",
    "formatar_valor",
    "formatar_percentual",
    "garantir_trigger_totais_saldos_caixas",
]
