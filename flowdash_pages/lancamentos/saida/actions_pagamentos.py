# flowdash_pages/lancamentos/saida/actions_pagamentos.py
"""
Actions de Pagamento (Fatura Cartão, Boleto e Empréstimo)
--------------------------------------------------------

Objetivo
--------
- Orquestrar pagamentos SEM atualizar CAP diretamente aqui.
- Preferir a API dos serviços de alto nível (pagar_*). Quando não usar o service,
  cair para os registradores de saída (registrar_saida_*), sempre informando
  `tipo_obrigacao` + `obrigacao_id` e os encargos.

Regras Financeiras (padrão vigente)
-----------------------------------
- Desconto abate PRIMEIRO o principal faltante (no CAP), mas NÃO sai do caixa.
- Dinheiro que sai do caixa/banco = principal_em_dinheiro + juros + multa.
- Status depende SOMENTE do principal acumulado vs valor_evento.

Reduções de redundância
-----------------------
- Listagens delegadas ao ContasAPagarMovRepository.
- Cálculos de “faltante/status” delegados ao repository.

Compat
------
- Mantém assinatura simples nas funções públicas `pagar_*_action`.
"""

from __future__ import annotations

from typing import Optional, Tuple, Dict, Any
from datetime import datetime

# Ledger / Services / Repository
from services.ledger.service_ledger import LedgerService
from services.ledger.service_ledger_boleto import ServiceLedgerBoleto
from services.ledger.service_ledger_emprestimo import ServiceLedgerEmprestimo
from services.ledger.service_ledger_fatura import ServiceLedgerFatura
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository


# =============================================================================
# Helpers internos (sem SQL direto)
# =============================================================================
def _get_services(caminho_banco: str) -> Tuple[LedgerService, ContasAPagarMovRepository]:
    """Factory simples para obter Ledger e Repository."""
    return LedgerService(caminho_banco), ContasAPagarMovRepository(caminho_banco)


def _canonicalizar_banco_safe(_: str, banco: Optional[str]) -> Optional[str]:
    """Normaliza nome de banco de forma defensiva (sem bater no DB)."""
    if not banco:
        return None
    nome = str(banco).strip()
    if not nome:
        return None
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


def _norm_forma(forma: Optional[str]) -> str:
    f = (forma or "").strip().upper()
    return "DÉBITO" if f == "DEBITO" else (f or "DINHEIRO")


def _norm_data(s: Optional[str]) -> str:
    if not s:
        return datetime.now().strftime("%Y-%m-%d")
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s).date().strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


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
    # Ligações CAP (genérico)
    tipo_obrigacao: str,
    obrigacao_id: int,
) -> Tuple[int, Optional[int]]:
    """
    Fallback padronizado para registrar a saída via Ledger:
    - Se forma = DINHEIRO => registrar_saida_dinheiro
    - Caso contrário => registrar_saida_bancaria
    Passa encargos e (`tipo_obrigacao`, `obrigacao_id`).
    Retorna (id_saida, id_mov) quando disponível.
    """
    forma_up = _norm_forma(forma_pagamento)
    kwargs_comuns: Dict[str, Any] = dict(
        data=_norm_data(data),
        valor=float(principal_cash),  # **somente principal em dinheiro**
        categoria=categoria,
        sub_categoria=sub_categoria,
        descricao=descricao,
        usuario=usuario,
        juros=float(juros or 0.0),
        multa=float(multa or 0.0),
        desconto=float(desconto or 0.0),
        tipo_obrigacao=tipo_obrigacao,
        obrigacao_id=int(obrigacao_id),
    )

    if forma_up == "DINHEIRO":
        kwargs_comuns["origem_dinheiro"] = origem_dinheiro or "Caixa"
        return ledger.registrar_saida_dinheiro(**kwargs_comuns)
    else:
        banco_nome = _canonicalizar_banco_safe(ledger.db_path, banco_nome)
        kwargs_comuns["banco_nome"] = banco_nome or "Banco Inter"
        kwargs_comuns["forma"] = forma_up
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
    forma_pagamento: str,             # "DINHEIRO" ou bancária (PIX/DÉBITO)
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
      1) Service direto (registra MB se chamado daqui)
      2) Fallback: registrar_saida_* com tipo_obrigacao='FATURA_CARTAO'
    """
    ledger, cap_repo = _get_services(caminho_banco)

    principal_cash, desconto_efetivo = _split_principal_e_desconto(
        cap_repo, int(obrigacao_id_fatura), float(valor_principal), float(desconto)
    )

    # 1) Service direto (garante log idempotente quando não há ledger_id)
    svc = ServiceLedgerFatura(caminho_banco)
    res = svc.pagar_fatura_cartao(
        obrigacao_id=int(obrigacao_id_fatura),
        valor_base=float(principal_cash),           # <- usar alias aceito
        juros=float(juros),
        multa=float(multa),
        desconto=float(desconto_efetivo),
        forma_pagamento=_norm_forma(forma_pagamento),
        origem=(origem_dinheiro if _norm_forma(forma_pagamento) == "DINHEIRO"
                else (_canonicalizar_banco_safe(caminho_banco, banco_nome) or "Banco 1")),
        data_evento=_norm_data(data),
        usuario=usuario,
    )

    # 2) Fallback apenas se service falhar de forma inesperada
    if not isinstance(res, dict) or res.get("ok") is False:
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
            tipo_obrigacao="FATURA_CARTAO",
            obrigacao_id=int(obrigacao_id_fatura),
        )
        res = {"id_saida": id_saida, "id_mov": id_mov, "ok": True}

    faltante, status = cap_repo.obter_restante_e_status(None, int(obrigacao_id_fatura))
    return {
        "ok": True,
        "principal_em_dinheiro": float(principal_cash),
        "juros": float(juros),
        "multa": float(multa),
        "desconto": float(desconto_efetivo),
        "restante": float(faltante),
        "status": status,
        **(res if isinstance(res, dict) else {}),
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
      1) Service direto (registra MB se chamado daqui)
      2) Fallback registrar_saida_* com tipo_obrigacao='BOLETO'
    """
    ledger, cap_repo = _get_services(caminho_banco)
    principal_cash, desconto_efetivo = _split_principal_e_desconto(
        cap_repo, int(obrigacao_id_boleto), float(valor_principal), float(desconto)
    )

    svc = ServiceLedgerBoleto(caminho_banco)
    res = svc.pagar_boleto(
        obrigacao_id=int(obrigacao_id_boleto),
        principal=float(principal_cash),
        juros=float(juros),
        multa=float(multa),
        desconto=float(desconto_efetivo),
        data_evento=_norm_data(data),
        usuario=usuario,
        forma_pagamento=_norm_forma(forma_pagamento),
        origem=(origem_dinheiro if _norm_forma(forma_pagamento) == "DINHEIRO"
                else (_canonicalizar_banco_safe(caminho_banco, banco_nome) or "Banco 1")),
    )

    if not isinstance(res, dict) or res.get("ok") is False:
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
            tipo_obrigacao="BOLETO",
            obrigacao_id=int(obrigacao_id_boleto),
        )
        res = {"id_saida": id_saida, "id_mov": id_mov, "ok": True}

    faltante, status = cap_repo.obter_restante_e_status(None, int(obrigacao_id_boleto))
    return {
        "ok": True,
        "principal_em_dinheiro": float(principal_cash),
        "juros": float(juros),
        "multa": float(multa),
        "desconto": float(desconto_efetivo),
        "restante": float(faltante),
        "status": status,
        **(res if isinstance(res, dict) else {}),
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
      1) Service direto (registra MB se chamado daqui)
      2) Fallback registrar_saida_* com tipo_obrigacao='EMPRESTIMO'
    """
    ledger, cap_repo = _get_services(caminho_banco)
    principal_cash, desconto_efetivo = _split_principal_e_desconto(
        cap_repo, int(obrigacao_id_emprestimo), float(valor_principal), float(desconto)
    )

    svc = ServiceLedgerEmprestimo(caminho_banco)
    res = svc.pagar_emprestimo(
        obrigacao_id=int(obrigacao_id_emprestimo),
        principal=float(principal_cash),
        juros=float(juros),
        multa=float(multa),
        desconto=float(desconto_efetivo),
        data_evento=_norm_data(data),
        usuario=usuario,
        forma_pagamento=_norm_forma(forma_pagamento),
        origem=(origem_dinheiro if _norm_forma(forma_pagamento) == "DINHEIRO"
                else (_canonicalizar_banco_safe(caminho_banco, banco_nome) or "Banco 1")),
    )

    if not isinstance(res, dict) or res.get("ok") is False:
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
            tipo_obrigacao="EMPRESTIMO",
            obrigacao_id=int(obrigacao_id_emprestimo),
        )
        res = {"id_saida": id_saida, "id_mov": id_mov, "ok": True}

    faltante, status = cap_repo.obter_restante_e_status(None, int(obrigacao_id_emprestimo))
    return {
        "ok": True,
        "principal_em_dinheiro": float(principal_cash),
        "juros": float(juros),
        "multa": float(multa),
        "desconto": float(desconto_efetivo),
        "restante": float(faltante),
        "status": status,
        **(res if isinstance(res, dict) else {}),
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
