# flowdash_pages/lancamentos/saida/actions_pagamentos.py
"""
Actions de Pagamento (Fatura Cartão, Boleto e Empréstimo)
--------------------------------------------------------

Objetivo
--------
- Orquestrar pagamentos SEM atualizar CAP diretamente aqui.
- Preferir a API de alto nível do Ledger (pagar_*) e,
  na ausência, cair para os registradores de saída (registrar_saida_*),
  sempre informando `obrigacao_id_*` e encargos (juros/multa/desconto).

Regras Financeiras (padrão vigente)
-----------------------------------
- Desconto abate PRIMEIRO o principal faltante.
- Dinheiro que sai do caixa/banco = principal_em_dinheiro + juros + multa.
- CAP acumula:
    principal_pago_acumulado += principal_em_dinheiro + desconto_efetivo
    juros_pago_acumulado     += juros
    multa_paga_acumulada     += multa
    desconto_aplicado_acumulado += desconto_efetivo
    valor_pago_acumulado (CAP, BRUTO) += principal_em_dinheiro + desconto + juros + multa
- Status depende SOMENTE do principal acumulado vs valor_evento.

Reduções de redundância
-----------------------
- Listagens (faturas/boletos/emp.) NÃO ficam mais aqui: delegamos ao
  `ContasAPagarMovRepository`.
- Cálculos de “faltante/status” NÃO ficam aqui: usamos o repository.

Compat
------
- Mantém assinatura simples nas funções públicas `pagar_*_action`.
"""

from __future__ import annotations

from typing import Optional, Tuple, Dict, Any
from datetime import datetime

# Ledger e Repository
from services.ledger.service_ledger import LedgerService
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository


# =============================================================================
# Helpers internos (sem SQL direto)
# =============================================================================
def _get_services(caminho_banco: str) -> Tuple[LedgerService, ContasAPagarMovRepository]:
    """Factory simples para obter Ledger e Repository."""
    return LedgerService(caminho_banco), ContasAPagarMovRepository(caminho_banco)


def _canonicalizar_banco_safe(caminho_banco: str, banco: Optional[str]) -> Optional[str]:
    """Normaliza nome de banco de forma defensiva (sem bater no DB)."""
    if not banco:
        return None
    nome = str(banco).strip()
    if not nome:
        return None
    # Normalizações usuais; ajuste conforme seus nomes canônicos
    aliases = {
        "INTER": "Banco Inter",
        "INFINITEPAY": "InfinitePay",
        "BRADESCO": "Bradesco",
        "CAIXA": "Caixa",
    }
    up = nome.upper()
    return aliases.get(up, nome)


def _split_principal_e_desconto(
    cap_repo: ContasAPagarMovRepository,
    obrigacao_id: int,
    principal_informado: float,
    desconto_informado: float,
) -> Tuple[float, float]:
    """
    Aplica o clamp correto:
    - desconto_efetivo <= faltante_principal
    - principal_cash <= faltante_principal - desconto_efetivo
    """
    faltante, _status = cap_repo.obter_restante_e_status(None, int(obrigacao_id))
    faltante = max(0.0, float(faltante or 0.0))

    desconto_efetivo = min(max(0.0, float(desconto_informado or 0.0)), faltante)
    restante_apos_desc = max(0.0, round(faltante - desconto_efetivo, 2))

    principal_cash = min(max(0.0, float(principal_informado or 0.0)), restante_apos_desc)
    return principal_cash, desconto_efetivo


def _registrar_saida_pagamento(
    ledger: LedgerService,
    *,
    data: str,
    principal_cash: float,
    forma_pagamento: str,
    banco_nome: Optional[str],
    origem_dinheiro: Optional[str],
    categoria: str,
    sub_categoria: Optional[str],
    descricao: str,
    usuario: str,
    # Encargos
    juros: float,
    multa: float,
    desconto: float,
    # Ligações CAP
    obrigacao_id_fatura: Optional[int] = None,
    obrigacao_id_boleto: Optional[int] = None,
    obrigacao_id_emprestimo: Optional[int] = None,
) -> Tuple[int, Optional[int]]:
    """
    Fallback padronizado para registrar a saída via Ledger:
    - Se forma = DINHEIRO => registrar_saida_dinheiro
    - Caso contrário => registrar_saida_bancaria
    Passa encargos e o `obrigacao_id_*` correspondente.
    Retorna (id_saida, id_mov) quando disponível.
    """
    forma_up = (forma_pagamento or "").strip().upper()
    kwargs_comuns: Dict[str, Any] = dict(
        data=data,
        valor=float(principal_cash),  # **somente principal em dinheiro**
        categoria=categoria,
        sub_categoria=sub_categoria,
        descricao=descricao,
        usuario=usuario,
        juros=float(juros or 0.0),
        multa=float(multa or 0.0),
        desconto=float(desconto or 0.0),
    )

    # Passa apenas um dos obrigacao_id_* (o que não for None)
    if obrigacao_id_fatura is not None:
        kwargs_comuns["obrigacao_id_fatura"] = int(obrigacao_id_fatura)
    if obrigacao_id_boleto is not None:
        kwargs_comuns["obrigacao_id_boleto"] = int(obrigacao_id_boleto)
    if obrigacao_id_emprestimo is not None:
        kwargs_comuns["obrigacao_id_emprestimo"] = int(obrigacao_id_emprestimo)

    if forma_up == "DINHEIRO":
        # Origem de dinheiro (ex.: "Caixa", "Caixa 2")
        kwargs_comuns["origem_dinheiro"] = origem_dinheiro or "Caixa"
        return ledger.registrar_saida_dinheiro(**kwargs_comuns)
    else:
        # Saída bancária (PIX/DÉBITO/CRÉDITO/BOLETO pago em conta)
        banco_nome = _canonicalizar_banco_safe(ledger.db_path, banco_nome)
        kwargs_comuns["banco_nome"] = banco_nome or "Banco Inter"
        kwargs_comuns["forma"] = forma_pagamento
        return ledger.registrar_saida_bancaria(**kwargs_comuns)


# =============================================================================
# Pagamentos Públicos (UI chama estas funções)
# =============================================================================
def pagar_fatura_cartao_action(
    *,
    caminho_banco: str,
    obrigacao_id_fatura: int,
    data: str,
    valor_principal: float,
    forma_pagamento: str,             # "DINHEIRO" ou bancária (PIX/DÉBITO/CRÉDITO)
    banco_nome: Optional[str] = None, # quando bancária
    origem_dinheiro: Optional[str] = None,  # quando dinheiro
    juros: float = 0.0,
    multa: float = 0.0,
    desconto: float = 0.0,
    descricao: str = "",
    usuario: str = "-",
    sub_categoria: Optional[str] = None,    # ex.: "Fatura Itaucard"
) -> Dict[str, Any]:
    """
    Paga fatura de cartão. Preferência:
      1) ledger.pagar_fatura_cartao (se existir)
      2) fallback: registrar_saida_* com obrigacao_id_fatura
    """
    ledger, cap_repo = _get_services(caminho_banco)

    # Clamp correto para principal e desconto (seguro mesmo se o Ledger já fizer isso)
    principal_cash, desconto_efetivo = _split_principal_e_desconto(
        cap_repo, int(obrigacao_id_fatura), float(valor_principal), float(desconto)
    )

    # 1) API de alto nível (se disponível)
    if hasattr(ledger, "pagar_fatura_cartao"):
        result = ledger.pagar_fatura_cartao(
            data=data,
            valor_principal=float(principal_cash),
            forma_pagamento=forma_pagamento,
            banco_nome=banco_nome,
            origem_dinheiro=origem_dinheiro,
            juros=float(juros),
            multa=float(multa),
            desconto=float(desconto_efetivo),
            obrigacao_id_fatura=int(obrigacao_id_fatura),
            descricao=descricao,
            usuario=usuario,
            sub_categoria=sub_categoria,
        )
    else:
        # 2) Fallback: registra saída e deixa o Ledger aplicar o CAP pelo obrigacao_id_*
        id_saida, id_mov = _registrar_saida_pagamento(
            ledger,
            data=data,
            principal_cash=principal_cash,
            forma_pagamento=forma_pagamento,
            banco_nome=banco_nome,
            origem_dinheiro=origem_dinheiro,
            categoria="Fatura Cartão de Crédito",
            sub_categoria=sub_categoria,
            descricao=descricao,
            usuario=usuario,
            juros=juros,
            multa=multa,
            desconto=desconto_efetivo,
            obrigacao_id_fatura=int(obrigacao_id_fatura),
        )
        result = {"id_saida": id_saida, "id_mov": id_mov}

    # Snapshot pós-pagamento para UI
    faltante, status = cap_repo.obter_restante_e_status(None, int(obrigacao_id_fatura))
    return {
        "ok": True,
        "principal_em_dinheiro": float(principal_cash),
        "juros": float(juros),
        "multa": float(multa),
        "desconto": float(desconto_efetivo),
        "restante": float(faltante),
        "status": status,
        **(result if isinstance(result, dict) else {}),
    }


def pagar_boleto_action(
    *,
    caminho_banco: str,
    obrigacao_id_boleto: int,
    data: str,
    valor_principal: float,
    forma_pagamento: str,
    banco_nome: Optional[str] = None,
    origem_dinheiro: Optional[str] = None,
    juros: float = 0.0,
    multa: float = 0.0,
    desconto: float = 0.0,
    descricao: str = "",
    usuario: str = "-",
    sub_categoria: Optional[str] = "Boleto",
) -> Dict[str, Any]:
    """
    Paga parcela de BOLETO. Preferência:
      1) ledger.pagar_parcela_boleto (se existir)
      2) fallback registrar_saida_* com obrigacao_id_boleto
    """
    ledger, cap_repo = _get_services(caminho_banco)
    principal_cash, desconto_efetivo = _split_principal_e_desconto(
        cap_repo, int(obrigacao_id_boleto), float(valor_principal), float(desconto)
    )

    if hasattr(ledger, "pagar_parcela_boleto"):
        result = ledger.pagar_parcela_boleto(
            data=data,
            valor_principal=float(principal_cash),
            forma_pagamento=forma_pagamento,
            banco_nome=banco_nome,
            origem_dinheiro=origem_dinheiro,
            juros=float(juros),
            multa=float(multa),
            desconto=float(desconto_efetivo),
            obrigacao_id_boleto=int(obrigacao_id_boleto),
            descricao=descricao,
            usuario=usuario,
            sub_categoria=sub_categoria,
        )
    else:
        id_saida, id_mov = _registrar_saida_pagamento(
            ledger,
            data=data,
            principal_cash=principal_cash,
            forma_pagamento=forma_pagamento,
            banco_nome=banco_nome,
            origem_dinheiro=origem_dinheiro,
            categoria="Pagamento de Boleto",
            sub_categoria=sub_categoria,
            descricao=descricao,
            usuario=usuario,
            juros=juros,
            multa=multa,
            desconto=desconto_efetivo,
            obrigacao_id_boleto=int(obrigacao_id_boleto),
        )
        result = {"id_saida": id_saida, "id_mov": id_mov}

    faltante, status = cap_repo.obter_restante_e_status(None, int(obrigacao_id_boleto))
    return {
        "ok": True,
        "principal_em_dinheiro": float(principal_cash),
        "juros": float(juros),
        "multa": float(multa),
        "desconto": float(desconto_efetivo),
        "restante": float(faltante),
        "status": status,
        **(result if isinstance(result, dict) else {}),
    }


def pagar_emprestimo_action(
    *,
    caminho_banco: str,
    obrigacao_id_emprestimo: int,
    data: str,
    valor_principal: float,
    forma_pagamento: str,
    banco_nome: Optional[str] = None,
    origem_dinheiro: Optional[str] = None,
    juros: float = 0.0,
    multa: float = 0.0,
    desconto: float = 0.0,
    descricao: str = "",
    usuario: str = "-",
    sub_categoria: Optional[str] = "Parcela Empréstimo",
) -> Dict[str, Any]:
    """
    Paga parcela de EMPRÉSTIMO. Preferência:
      1) ledger.pagar_parcela_emprestimo (se existir)
      2) fallback registrar_saida_* com obrigacao_id_emprestimo
    """
    ledger, cap_repo = _get_services(caminho_banco)
    principal_cash, desconto_efetivo = _split_principal_e_desconto(
        cap_repo, int(obrigacao_id_emprestimo), float(valor_principal), float(desconto)
    )

    if hasattr(ledger, "pagar_parcela_emprestimo"):
        result = ledger.pagar_parcela_emprestimo(
            data=data,
            valor_principal=float(principal_cash),
            forma_pagamento=forma_pagamento,
            banco_nome=banco_nome,
            origem_dinheiro=origem_dinheiro,
            juros=float(juros),
            multa=float(multa),
            desconto=float(desconto_efetivo),
            obrigacao_id_emprestimo=int(obrigacao_id_emprestimo),
            descricao=descricao,
            usuario=usuario,
            sub_categoria=sub_categoria,
        )
    else:
        id_saida, id_mov = _registrar_saida_pagamento(
            ledger,
            data=data,
            principal_cash=principal_cash,
            forma_pagamento=forma_pagamento,
            banco_nome=banco_nome,
            origem_dinheiro=origem_dinheiro,
            categoria="Pagamento de Empréstimo",
            sub_categoria=sub_categoria,
            descricao=descricao,
            usuario=usuario,
            juros=juros,
            multa=multa,
            desconto=desconto_efetivo,
            obrigacao_id_emprestimo=int(obrigacao_id_emprestimo),
        )
        result = {"id_saida": id_saida, "id_mov": id_mov}

    faltante, status = cap_repo.obter_restante_e_status(None, int(obrigacao_id_emprestimo))
    return {
        "ok": True,
        "principal_em_dinheiro": float(principal_cash),
        "juros": float(juros),
        "multa": float(multa),
        "desconto": float(desconto_efetivo),
        "restante": float(faltante),
        "status": status,
        **(result if isinstance(result, dict) else {}),
    }


# =============================================================================
# Listagens simplificadas (sem duplicação de SQL)
# =============================================================================
def listar_faturas_abertas(caminho_banco: str):
    _, cap_repo = _get_services(caminho_banco)
    return cap_repo.listar_faturas_cartao_abertas()


def listar_boletos_abertos(caminho_banco: str):
    _, cap_repo = _get_services(caminho_banco)
    return cap_repo.listar_boletos_em_aberto()


def listar_emprestimos_abertos(caminho_banco: str):
    _, cap_repo = _get_services(caminho_banco)
    return cap_repo.listar_emprestimos_em_aberto()
