"""
Páginas de Cadastros
====================

Agrupa as páginas de cadastro (bancos, caixa, cartões, metas, etc.).
Importe módulos específicos quando precisar.
Ex.: from cadastros import pagina_bancos_cadastrados
"""

from . import (
    cadastro_categorias,
    cadastro_classes,
    pagina_bancos_cadastrados,
    pagina_caixa,
    pagina_cartoes,
    pagina_correcao_caixa,
    pagina_emprestimos,
    pagina_maquinas,
    pagina_metas,
    pagina_saldos_bancarios,
    pagina_usuarios,
)

__all__ = [
    "cadastro_categorias",
    "cadastro_classes",
    "pagina_bancos_cadastrados",
    "pagina_caixa",
    "pagina_cartoes",
    "pagina_correcao_caixa",
    "pagina_emprestimos",
    "pagina_maquinas",
    "pagina_metas",
    "pagina_saldos_bancarios",
    "pagina_usuarios",
]
