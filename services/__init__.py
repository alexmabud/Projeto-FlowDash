"""
Pacote Services
===============

Camada de serviços de domínio do FlowDash.

Subpacotes e módulos
--------------------
- ledger ....... regras de negócio para lançamentos financeiros (dividido em mixins).
- taxas ........ consultas e regras relacionadas às taxas de maquinetas.
- vendas ....... serviços utilitários para vendas.

Observação:
    - Módulos de backup (`ledger_backup.py`) existem apenas para referência
      e não fazem parte da API pública principal.
"""

from __future__ import annotations

from . import ledger, taxas, vendas

__all__ = ["ledger", "taxas", "vendas"]
