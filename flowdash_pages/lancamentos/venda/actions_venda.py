# ===================== Actions: Venda =====================
"""
Executa a MESMA lógica do módulo original de Vendas (sem Streamlit aqui):
- Validações de campos
- Cálculo de taxa/banco_destino a partir de taxas_maquinas
- Suporte a PIX via maquineta ou direto para banco (com taxa informada)
- Cálculo de data de liquidação (DIAS_COMPENSACAO + proximo dia útil)
- Chamada do VendasService.registrar_venda
"""

from __future__ import annotations

import pandas as pd
from shared.db import get_conn
from services.vendas import VendasService
from utils.utils import formatar_valor
from flowdash_pages.lancamentos.shared_ui import (
    DIAS_COMPENSACAO,
    proximo_dia_util_br,
    obter_banco_destino,
)

def _r2(x) -> float:
    """Arredonda em 2 casas para evitar ruídos (ex.: -0,00)."""
    return round(float(x or 0.0), 2)

def _formas_equivalentes(forma: str):
    forma = (forma or "").upper()
    if forma == "LINK_PAGAMENTO":
        return ["LINK_PAGAMENTO", "LINK PAGAMENTO", "LINK-DE-PAGAMENTO", "LINK DE PAGAMENTO"]
    return [forma]

def _descobrir_taxa_e_banco(caminho_banco: str, forma: str, maquineta: str, bandeira: str, parcelas: int, modo_pix: str | None, banco_pix_direto: str | None, taxa_pix_direto: float) -> tuple[float, str | None]:
    """
    Replica exatamente a lógica do módulo original para determinar taxa% e banco_destino.
    """
    taxa, banco_destino = 0.0, None

    if forma in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"]:
        formas = _formas_equivalentes(forma)
        placeholders = ",".join(["?"] * len(formas))
        with get_conn(caminho_banco) as conn:
            row = conn.execute(
                f"""
                SELECT taxa_percentual, banco_destino FROM taxas_maquinas
                WHERE UPPER(forma_pagamento) IN ({placeholders}) AND maquineta=? AND bandeira=? AND parcelas=?
                LIMIT 1
                """,
                [f.upper() for f in formas] + [maquineta, bandeira, int(parcelas or 1)]
            ).fetchone()
        if row:
            taxa = float(row[0] or 0.0)
            banco_destino = row[1] or None
        if not banco_destino:
            banco_destino = obter_banco_destino(caminho_banco, forma, maquineta, bandeira, parcelas)

    elif forma == "PIX":
        if (modo_pix or "") == "Via maquineta":
            with get_conn(caminho_banco) as conn:
                row = conn.execute(
                    """
                    SELECT taxa_percentual, banco_destino FROM taxas_maquinas
                    WHERE UPPER(forma_pagamento)='PIX' AND maquineta=? AND bandeira='' AND parcelas=1
                    LIMIT 1
                    """,
                    (maquineta,)
                ).fetchone()
            taxa = float(row[0] or 0.0) if row else 0.0
            banco_destino = (row[1] if row and row[1] else None) or obter_banco_destino(caminho_banco, "PIX", maquineta, "", 1)
        else:
            banco_destino = banco_pix_direto
            taxa = float(taxa_pix_direto or 0.0)

    else:  # DINHEIRO
        banco_destino, taxa, parcelas, bandeira, maquineta = None, 0.0, 1, "", ""

    return _r2(taxa), (banco_destino or None)

def registrar_venda(caminho_banco: str, data_lanc, payload: dict) -> dict:
    """
    Registra a venda de acordo com a forma/parametrizações — mesma lógica do original.
    Retorna dict com {ok: bool, msg: str}.
    """
    valor = float(payload.get("valor") or 0.0)
    forma = (payload.get("forma") or "").strip()
    parcelas = int(payload.get("parcelas") or 1)
    bandeira = (payload.get("bandeira") or "").strip()
    maquineta = (payload.get("maquineta") or "").strip()
    modo_pix = payload.get("modo_pix")
    banco_pix_direto = payload.get("banco_pix_direto")
    taxa_pix_direto = float(payload.get("taxa_pix_direto") or 0.0)

    # --- Validações idênticas ---
    if valor <= 0:
        raise ValueError("Valor inválido.")
    if forma in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"] and (not maquineta or not bandeira):
        raise ValueError("Selecione maquineta e bandeira.")
    if forma == "PIX" and (modo_pix or "") == "Via maquineta" and not maquineta:
        raise ValueError("Selecione a maquineta do PIX.")
    if forma == "PIX" and (modo_pix or "") == "Direto para banco" and not banco_pix_direto:
        raise ValueError("Selecione o banco que receberá o PIX direto.")

    # taxa + banco_destino
    taxa, banco_destino = _descobrir_taxa_e_banco(
        caminho_banco=caminho_banco,
        forma=forma,
        maquineta=maquineta,
        bandeira=bandeira,
        parcelas=parcelas,
        modo_pix=modo_pix,
        banco_pix_direto=banco_pix_direto,
        taxa_pix_direto=taxa_pix_direto,
    )

    # data de liquidação
    base = pd.to_datetime(data_lanc).date()
    dias = DIAS_COMPENSACAO.get(forma, 0)
    data_liq = proximo_dia_util_br(base, dias) if dias > 0 else base

    # service
    service = VendasService(caminho_banco)
    venda_id, mov_id = service.registrar_venda(
        data_venda=str(data_lanc),
        data_liq=str(data_liq),
        valor_bruto=_r2(valor),
        forma=forma,
        parcelas=int(parcelas or 1),
        bandeira=bandeira or "",
        maquineta=maquineta or "",
        banco_destino=banco_destino,
        taxa_percentual=_r2(taxa or 0.0),
        usuario=( "Sistema" )  # o original usa usuario_logado["nome"] no page; aqui deixamos placeholder
    )

    if venda_id == -1:
        msg = "⚠️ Venda já registrada (idempotência)."
    else:
        valor_liq = _r2(float(valor) * (1 - float(taxa or 0.0) / 100.0))
        msg_liq = (
            f"Liquidação de {formatar_valor(valor_liq)} em {(banco_destino or 'Caixa')} "
            f"em {pd.to_datetime(data_liq).strftime('%d/%m/%Y')}"
        )
        msg = f"✅ Venda registrada! {msg_liq}"

    return {"ok": True, "msg": msg}
