"""
Pacote repository
=================

Este pacote concentra os **repositórios de acesso ao banco de dados** do FlowDash.
Cada repositório encapsula operações de leitura, escrita e atualização em tabelas
específicas do SQLite, fornecendo uma interface consistente para os módulos de
serviço e páginas de lançamentos.

Repositórios principais
-----------------------
- MovimentacoesRepository .............. movimentações bancárias
- CategoriasRepository ................. categorias de lançamentos
- CartoesRepository .................... cartões e faturas
- BancosCadastradosRepository .......... bancos cadastrados no sistema
- EmprestimosFinanciamentosRepository .. empréstimos e financiamentos
- TaxasMaquinasRepository .............. taxas de máquinas de cartão
- contas_a_pagar_mov_repository ........ subpacote especializado em contas a pagar
"""

from repository.movimentacoes_repository import MovimentacoesRepository
from repository.categorias_repository import CategoriasRepository
from repository.cartoes_repository import CartoesRepository
from repository.bancos_cadastrados_repository import BancosCadastradosRepository
from repository.emprestimos_financiamentos_repository import EmprestimosFinanciamentosRepository
from repository.taxas_maquinas_repository import TaxasMaquinasRepository
from repository import contas_a_pagar_mov_repository

__all__ = [
    "MovimentacoesRepository",
    "CategoriasRepository",
    "CartoesRepository",
    "BancosCadastradosRepository",
    "EmprestimosFinanciamentosRepository",
    "TaxasMaquinasRepository",
    "contas_a_pagar_mov_repository",
]
