"""
Página: Lançamentos / Saída
===========================

Organiza a página, estado, formulários e ações da **Saída**:
- DINHEIRO (Caixa / Caixa 2)
- PIX / DÉBITO (bancos)
- CRÉDITO (parcelado por fatura)
- BOLETO (parcelado)

"""

"""Página de Saídas (Lançamentos)"""
from .page_saida import render_saida
__all__ = ["render_saida"]
