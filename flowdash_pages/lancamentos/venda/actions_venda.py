# ===================== Actions: Venda =====================
"""
Executa a MESMA lógica do módulo original de Vendas (sem Streamlit aqui):
- Validações de campos
- Cálculo de taxa/banco_destino a partir de taxas_maquinas
- Suporte a PIX via maquineta ou direto para banco (com taxa informada)
- Cálculo de data de liquidação (DIAS_COMPENSACAO + próximo dia útil)
- Chamada do serviço de vendas (VendasService); fallback para LedgerService se necessário
"""

from __future__ import annotations

from typing import Optional, Tuple, Any
import pandas as pd

from shared.db import get_conn
from utils.utils import formatar_valor
from flowdash_pages.lancamentos.shared_ui import (
    DIAS_COMPENSACAO,
    proximo_dia_util_br,
    obter_banco_destino,
)

# ---- Serviço de vendas: usa services.vendas se existir; fallback para services.ledger ----
_VendasService = None
try:
    # Projeto com serviço dedicado de vendas
    from services.vendas import VendasService as _VendasService  # type: ignore
except Exception:
    try:
        # Projeto que centraliza tudo no Ledger
        from services.ledger import LedgerService as _VendasService  # type: ignore
    except Exception:
        _VendasService = None  # será checado em runtime


def _r2(x) -> float:
    """Arredonda em 2 casas para evitar ruídos (ex.: -0,00)."""
    return round(float(x or 0.0), 2)


def _formas_equivalentes(forma: str):
    f = (forma or "").upper()
    if f == "LINK_PAGAMENTO":
        return ["LINK_PAGAMENTO", "LINK PAGAMENTO", "LINK-DE-PAGAMENTO", "LINK DE PAGAMENTO"]
    return [f]


def _descobrir_taxa_e_banco(
    db_like: Any,
    forma: str,
    maquineta: str,
    bandeira: str,
    parcelas: int,
    modo_pix: Optional[str],
    banco_pix_direto: Optional[str],
    taxa_pix_direto: float,
) -> Tuple[float, Optional[str]]:
    """
    Mesma lógica do módulo original para determinar taxa% e banco_destino.
    Usa a tabela taxas_maquinas (colunas: forma_pagamento, maquineta, bandeira, parcelas, taxa_percentual, banco_destino).
    """
    taxa, banco_destino = 0.0, None
    forma_up = (forma or "").upper()

    if forma_up in ["DÉBITO", "CREDITO", "CRÉDITO", "LINK_PAGAMENTO"]:
        # normaliza 'CREDITO' -> 'CRÉDITO' se vier sem acento
        forma_norm = "CRÉDITO" if forma_up in ("CREDITO", "CRÉDITO") else forma_up
        formas = _formas_equivalentes(forma_norm)
        placeholders = ",".join(["?"] * len(formas))
        with get_conn(db_like) as conn:
            row = conn.execute(
                f"""
                SELECT taxa_percentual, banco_destino FROM taxas_maquinas
                WHERE UPPER(forma_pagamento) IN ({placeholders})
                  AND maquineta=? AND bandeira=? AND parcelas=?
                LIMIT 1
                """,
                [f.upper() for f in formas] + [maquineta, bandeira, int(parcelas or 1)],
            ).fetchone()
        if row:
            taxa = float(row[0] or 0.0)
            banco_destino = row[1] or None
        if not banco_destino:
            banco_destino = obter_banco_destino(db_like, forma_norm, maquineta, bandeira, parcelas)

    elif forma_up == "PIX":
        if (modo_pix or "") == "Via maquineta":
            with get_conn(db_like) as conn:
                row = conn.execute(
                    """
                    SELECT taxa_percentual, banco_destino FROM taxas_maquinas
                    WHERE UPPER(forma_pagamento)='PIX'
                      AND maquineta=? AND bandeira='' AND parcelas=1
                    LIMIT 1
                    """,
                    (maquineta,),
                ).fetchone()
            taxa = float(row[0] or 0.0) if row else 0.0
            banco_destino = (row[1] if row and row[1] else None) or obter_banco_destino(
                db_like, "PIX", maquineta, "", 1
            )
        else:
            banco_destino = banco_pix_direto
            taxa = float(taxa_pix_direto or 0.0)

    else:  # DINHEIRO
        banco_destino, taxa, parcelas = None, 0.0, 1  # simples e direto

    return _r2(taxa), (banco_destino or None)


def _chamar_service_registrar_venda(
    service: Any,
    *,
    db_like: Any,
    data_venda: str,
    data_liq: str,
    valor: float,
    forma: str,
    parcelas: int,
    bandeira: str,
    maquineta: str,
    banco_destino: Optional[str],
    taxa_percentual: float,
    usuario: str,
):
    """
    Tenta registrar_venda em assinaturas diferentes, nesta ordem:
      1) API moderna (com db_like e nomes 'data', 'valor', 'forma_pagamento', 'data_liq')
      2) API moderna sem db_like (passado no __init__)
      3) API antiga ('data_venda', 'valor_bruto', 'forma')
    Retorna (venda_id, mov_id).
    """
    last_type_error = None

    # 1) Moderna + db_like
    try:
        return service.registrar_venda(
            db_like=db_like,
            data=data_venda,
            valor=valor,
            forma_pagamento=forma,
            parcelas=int(parcelas or 1),
            bandeira=bandeira or "",
            maquineta=maquineta or "",
            banco_destino=banco_destino,
            taxa_percentual=_r2(taxa_percentual or 0.0),
            data_liq=data_liq,
            usuario=usuario,
        )
    except TypeError as e:
        last_type_error = e
    except Exception:
        raise  # deixe erros reais aparecerem

    # 2) Moderna sem db_like
    try:
        return service.registrar_venda(
            data=data_venda,
            valor=valor,
            forma_pagamento=forma,
            parcelas=int(parcelas or 1),
            bandeira=bandeira or "",
            maquineta=maquineta or "",
            banco_destino=banco_destino,
            taxa_percentual=_r2(taxa_percentual or 0.0),
            data_liq=data_liq,
            usuario=usuario,
        )
    except TypeError as e:
        last_type_error = e
    except Exception:
        raise

    # 3) Antiga (valor_bruto/forma/data_venda)
    try:
        return service.registrar_venda(
            data_venda=data_venda,
            data_liq=data_liq,
            valor_bruto=_r2(valor),
            forma=forma,
            parcelas=int(parcelas or 1),
            bandeira=bandeira or "",
            maquineta=maquineta or "",
            banco_destino=banco_destino,
            taxa_percentual=_r2(taxa_percentual or 0.0),
            usuario=usuario,
        )
    except TypeError as e:
        last_type_error = e
    except Exception:
        raise

    # Se todas falharam por incompatibilidade de assinatura:
    raise last_type_error or TypeError("Assinatura de registrar_venda incompatível com as tentativas conhecidas.")


def _extrair_nome_simples(x: Any) -> str | None:
    """Normaliza para um 'nome' simples (sem domínio do e-mail)."""
    if not x:
        return None
    s = str(x).strip()
    # se vier e-mail, pega antes do @
    if "@" in s and " " not in s:
        s = s.split("@", 1)[0]
    # se quiser apenas o primeiro nome, descomente:
    # s = s.split()[0]
    return s or None


def registrar_venda(*, db_like: Any = None, data_lanc=None, payload: dict | None = None, **kwargs) -> dict:
    """
    Registra a venda (compatível com chamadas legadas).
    Usa a data selecionada na tela como Data da VENDA.
    """
    # Compat: permitir chamadas antigas com 'caminho_banco'
    if db_like is None and "caminho_banco" in kwargs:
        db_like = kwargs.pop("caminho_banco")

    payload = payload or {}

    # ------- campos do payload -------
    valor = float(payload.get("valor") or 0.0)
    forma = (payload.get("forma") or "").strip().upper()
    if forma == "CREDITO":
        forma = "CRÉDITO"
    parcelas = int(payload.get("parcelas") or 1)
    bandeira = (payload.get("bandeira") or "").strip()
    maquineta = (payload.get("maquineta") or "").strip()
    modo_pix = payload.get("modo_pix")
    banco_pix_direto = payload.get("banco_pix_direto")

    # ⚠️ PIX direto: ignorar qualquer taxa do formulário (padroniza em 0.0)
    taxa_pix_direto = 0.0

    # ------- validações -------
    if valor <= 0:
        raise ValueError("Valor inválido.")
    if forma in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"] and (not maquineta or not bandeira):
        raise ValueError("Selecione maquineta e bandeira.")
    if forma == "PIX" and (modo_pix or "") == "Via maquineta" and not maquineta:
        raise ValueError("Selecione a maquineta do PIX.")
    if forma == "PIX" and (modo_pix or "") == "Direto para banco" and not banco_pix_direto:
        raise ValueError("Selecione o banco que receberá o PIX direto.")

    # ------- taxa + banco_destino -------
    taxa, banco_destino = _descobrir_taxa_e_banco(
        db_like=db_like,
        forma=forma,
        maquineta=maquineta,
        bandeira=bandeira,
        parcelas=parcelas,
        modo_pix=modo_pix,
        banco_pix_direto=banco_pix_direto,
        taxa_pix_direto=taxa_pix_direto,  # <- forçado 0.0 para PIX direto
    )

    # ------- datas -------
    # Data da VENDA = data selecionada na tela
    data_venda_str = pd.to_datetime(data_lanc).strftime("%Y-%m-%d")

    # Data de liquidação = compensação + próximo dia útil (se houver), senão mesma data
    base = pd.to_datetime(data_lanc).date()
    dias = DIAS_COMPENSACAO.get(forma, 0)
    data_liq_date = proximo_dia_util_br(base, dias) if dias > 0 else base
    data_liq_str = pd.to_datetime(data_liq_date).strftime("%Y-%m-%d")

    # ------- usuário (nome simples) -------
    usuario_atual = payload.get("usuario")
    if not usuario_atual:
        try:
            import streamlit as st
            u = st.session_state.get("usuario_logado")
            if isinstance(u, dict):
                usuario_atual = u.get("nome") or u.get("nome_completo") or u.get("usuario") or u.get("email")
            else:
                usuario_atual = getattr(u, "nome", None) or getattr(u, "nome_completo", None) \
                                or getattr(u, "usuario", None) or getattr(u, "email", None) \
                                or (u if isinstance(u, str) else None)
        except Exception:
            pass
    usuario_atual = _extrair_nome_simples(usuario_atual) or "Sistema"

    # ------- serviço -------
    if _VendasService is None:
        raise RuntimeError(
            "Serviço de Vendas não encontrado. Tenha `services.vendas.VendasService` "
            "ou `services.ledger.LedgerService` disponível."
        )
    try:
        service = _VendasService(db_like=db_like)
    except TypeError:
        service = _VendasService(db_like)

    if not hasattr(service, "registrar_venda"):
        raise RuntimeError("O serviço carregado não expõe `registrar_venda(...)`.")

    # ------- chamada ao service (força Data da VENDA) -------
    venda_id, mov_id = _chamar_service_registrar_venda(
        service,
        db_like=db_like,
        data_venda=data_venda_str,   # <- AQUI garantimos a Data da VENDA
        data_liq=data_liq_str,
        valor=_r2(valor),
        forma=forma,
        parcelas=int(parcelas or 1),
        bandeira=bandeira or "",
        maquineta=maquineta or "",
        banco_destino=banco_destino,
        taxa_percentual=_r2(taxa or 0.0),
        usuario=usuario_atual,
    )

    # ------- retorno -------
    from utils.utils import formatar_valor
    if venda_id == -1:
        msg = "⚠️ Venda já registrada (idempotência)."
    else:
        valor_liq = _r2(float(valor) * (1 - float(taxa or 0.0) / 100.0))
        msg_liq = (
            f"Liquidação de {formatar_valor(valor_liq)} em {(banco_destino or 'Caixa_Vendas')}"
            f" em {pd.to_datetime(data_liq_str).strftime('%d/%m/%Y')}"
        )
        msg = f"✅ Venda registrada! {msg_liq}"

    return {"ok": True, "msg": msg}

