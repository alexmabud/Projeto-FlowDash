# flowdash_pages/lancamentos/saida/actions_saida.py
# ===================== Actions: Saída =====================
"""
Actions da página de Saída (sem Streamlit aqui).

Refatoração (2025-09-04)
------------------------

- Delegação total de listagens e regras de CAP ao Repository.
- Pagamentos específicos (fatura/boletos/emprestimos) ficam em
  `flowdash_pages.lancamentos.saida.actions_pagamentos`.
- Aqui ficamos com:
    • Registrar saídas genéricas (dinheiro/bancária) sem CAP.
    • Programar BOLETO (criar LANCAMENTO no CAP).
    • Pequenos wrappers de listagem (delegando ao Repository) para
      manter compatibilidade com a UI.

Regras
------
- Saída genérica:
    ledger.registrar_saida_dinheiro(...)    # quando forma="DINHEIRO"
    ledger.registrar_saida_bancaria(...)    # quando forma ≠ "DINHEIRO"
- Programação de boleto:
    repository.registrar_lancamento(..., tipo_obrigacao="BOLETO", categoria_evento="LANCAMENTO")
"""

from __future__ import annotations

from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime

# Ledger e Repository
from services.ledger.service_ledger import LedgerService
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository

# Reuso de helper (evita duplicação)
from flowdash_pages.lancamentos.saida.actions_pagamentos import (  # type: ignore
    _canonicalizar_banco_safe,  # reuso intencional
)

__all__ = [
    "carregar_listas_para_form",
    "registrar_saida",
    "programar_boleto",
    "listar_boletos_em_aberto",
    "_listar_boletos_em_aberto",
]

# ---------- Constantes (somente UI)
FORMAS = ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "BOLETO"]
ORIGENS_DINHEIRO_PADRAO = ["Caixa", "Caixa 2"]


# =============================================================================
# Factory
# =============================================================================
def _get_services(caminho_banco: str) -> Tuple[LedgerService, ContasAPagarMovRepository]:
    return LedgerService(caminho_banco), ContasAPagarMovRepository(caminho_banco)


# =============================================================================
# Listas para o formulário (somente UI)
# =============================================================================
def carregar_listas_para_form(caminho_banco: str):
    """
    Retorna tupla no formato legado (8 itens), preservando o mesmo comportamento do formulário:
    (nomes_bancos, nomes_cartoes, df_categorias, listar_subcategorias,
     listar_destinos_fatura_em_aberto, opcoes_pagamentos,
     listar_boletos_em_aberto, listar_empfin_em_aberto)
    """
    # Imports locais para evitar mexer no topo do arquivo
    import pandas as pd  # noqa: F401
    from flowdash_pages.cadastros.cadastro_classes import BancoRepository
    from repository.cartoes_repository import CartoesRepository
    from repository.categorias_repository import CategoriasRepository
    from flowdash_pages.lancamentos.saida.actions_pagamentos import (
        listar_faturas_abertas,
        listar_boletos_abertos,
        listar_emprestimos_abertos,
    )

    # Repositórios originais (mantém UI idêntica)
    bancos_repo = BancoRepository(caminho_banco)
    cartoes_repo = CartoesRepository(caminho_banco)
    cats_repo = CategoriasRepository(caminho_banco)

    df_bancos = bancos_repo.carregar_bancos()
    nomes_bancos = df_bancos["nome"].tolist() if (df_bancos is not None and not df_bancos.empty) else []
    nomes_cartoes = cartoes_repo.listar_nomes()
    df_categorias = cats_repo.listar_categorias()

    # Callbacks esperados pelo formulário (mesma ordem/assinatura)
    listar_subcategorias = cats_repo.listar_subcategorias
    listar_destinos_fatura_em_aberto = lambda: listar_faturas_abertas(caminho_banco)
    opcoes_pagamentos = lambda tipo: FORMAS  # usa a constante já definida no arquivo
    listar_boletos = lambda: listar_boletos_abertos(caminho_banco)
    listar_empfin = lambda: listar_emprestimos_abertos(caminho_banco)

    return (
        nomes_bancos,
        nomes_cartoes,
        df_categorias,
        listar_subcategorias,
        listar_destinos_fatura_em_aberto,
        opcoes_pagamentos,
        listar_boletos,
        listar_empfin,
    )

# =============================================================================
# Saída genérica (sem CAP)
# =============================================================================
def registrar_saida(
    *,
    caminho_banco: str,
    data: str,
    valor: float,
    forma_pagamento: str,                  # "DINHEIRO", "PIX", "DÉBITO", "CRÉDITO"
    categoria: str,
    descricao: str,
    usuario: str = "-",
    sub_categoria: Optional[str] = None,
    origem_dinheiro: Optional[str] = None,  # quando DINHEIRO
    banco_nome: Optional[str] = None,       # quando não DINHEIRO
) -> Dict[str, Any]:
    """
    Registra uma SAÍDA genérica (não vinculada a CAP).

    - Quando forma = "DINHEIRO": usa `registrar_saida_dinheiro`.
    - Caso contrário: usa `registrar_saida_bancaria`.
    """
    ledger, _ = _get_services(caminho_banco)
    data_str = str(data or datetime.now().strftime("%Y-%m-%d"))
    valor_f = float(valor or 0.0)

    if (forma_pagamento or "").strip().upper() == "DINHEIRO":
        origem = (origem_dinheiro or ORIGENS_DINHEIRO_PADRAO[0])
        saida_id, mov_id = ledger.registrar_saida_dinheiro(
            data=data_str,
            valor=valor_f,
            origem_dinheiro=origem,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao,
            usuario=usuario,
        )
    else:
        banco_norm = _canonicalizar_banco_safe(caminho_banco, banco_nome)
        saida_id, mov_id = ledger.registrar_saida_bancaria(
            data=data_str,
            valor=valor_f,
            banco_nome=banco_norm or "Banco Inter",
            forma=forma_pagamento,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao,
            usuario=usuario,
        )

    return {
        "ok": True,
        "id_saida": saida_id,
        "id_mov": mov_id,
        "data": data_str,
        "valor": valor_f,
        "forma": forma_pagamento,
        "categoria": categoria,
        "sub_categoria": sub_categoria,
        "descricao": descricao,
        "usuario": usuario,
    }


# =============================================================================
# Programação de BOLETO (gera LANCAMENTO no CAP)
# =============================================================================
def programar_boleto(
    *,
    caminho_banco: str,
    valor_total: float,
    vencimento: str,
    credor: Optional[str],
    descricao: Optional[str],
    usuario: str,
    competencia: Optional[str] = None,
    parcela_num: Optional[int] = None,
    parcelas_total: Optional[int] = None,
    obrigacao_id: Optional[int] = None,
    data_evento: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Cria/insere um LANCAMENTO de BOLETO no CAP.
    - Se não informar `obrigacao_id`, gera o próximo automaticamente.
    - Acumuladores começam zerados e `status = EM ABERTO`.
    """
    _, repo = _get_services(caminho_banco)

    _obrigacao_id = int(obrigacao_id or repo.proximo_obrigacao_id(None))
    lanc_id = repo.registrar_lancamento(
        None,
        obrigacao_id=_obrigacao_id,
        tipo_obrigacao="BOLETO",
        valor_total=float(valor_total or 0.0),
        data_evento=str(data_evento or datetime.now().strftime("%Y-%m-%d")),
        vencimento=str(vencimento),
        descricao=(descricao or None),
        credor=(credor or None),
        competencia=(competencia or None),
        parcela_num=(int(parcela_num) if parcela_num else None),
        parcelas_total=(int(parcelas_total) if parcelas_total else None),
        usuario=str(usuario or "-"),
        tipo_origem=None,
        cartao_id=None,
        emprestimo_id=None,
    )

    return {
        "ok": True,
        "lancamento_id": int(lanc_id),
        "obrigacao_id": int(_obrigacao_id),
        "tipo_obrigacao": "BOLETO",
        "vencimento": str(vencimento),
        "valor_total": float(valor_total or 0.0),
        "credor": (credor or None),
        "descricao": (descricao or None),
        "usuario": str(usuario or "-"),
    }


# =============================================================================
# Listagens — delegação ao Repository (mantém compat com UI)
# =============================================================================
def listar_boletos_em_aberto(caminho_banco: str) -> List[dict]:
    _, repo = _get_services(caminho_banco)
    return repo.listar_boletos_em_aberto()


# Alias compat (nome legado)
def _listar_boletos_em_aberto(caminho_banco: str) -> List[dict]:
    return listar_boletos_em_aberto(caminho_banco)
