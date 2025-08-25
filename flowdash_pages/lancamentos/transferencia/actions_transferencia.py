# ===================== Actions: Transferência =====================
"""
Executa a lógica da **transferência banco → banco**:

- Canonicaliza nomes dos bancos (mantendo consistência com `saldos_bancos`)
- Registra **duas linhas** em `movimentacoes_bancarias`:
    1) SAÍDA no banco de ORIGEM (tipo='saida', origem='transferencia')
    2) ENTRADA no banco de DESTINO (tipo='entrada', origem='transferencia')
- Atualiza `saldos_bancos` no mesmo dia:
    - decrementa coluna do banco de ORIGEM
    - incrementa coluna do banco de DESTINO

Observações:
- Se não for possível calcular o saldo atual do banco de origem, o sistema **não bloqueia**,
  apenas segue com a transferência (como no comportamento tolerante dos outros fluxos).
"""

from __future__ import annotations

import uuid
from typing import TypedDict, Optional
import pandas as pd
from datetime import date as _date, datetime as _dt

from utils.utils import coerce_data  # <<< normaliza data
from repository.movimentacoes_repository import MovimentacoesRepository  # <<< usa repo p/ respeitar CHECK

from shared.db import get_conn
from flowdash_pages.cadastros.cadastro_classes import BancoRepository
from flowdash_pages.lancamentos.shared_ui import upsert_saldos_bancos, canonicalizar_banco


class ResultadoTransferenciaBancos(TypedDict):
    ok: bool
    msg: str
    origem: str
    destino: str
    valor: float


def carregar_nomes_bancos(caminho_banco: str) -> list[str]:
    """Lista de bancos cadastrados (mesmo comportamento do Depósito)."""
    repo = BancoRepository(caminho_banco)
    df = repo.carregar_bancos()
    return df["nome"].tolist() if df is not None and not df.empty else []


def _try_saldo_banco(caminho_banco: str, banco_nome: str, data_str: str) -> Optional[float]:
    """
    Tenta calcular o saldo acumulado (≤ data) do banco indicado em `saldos_bancos`.
    Funciona se o modelo for wide-table (coluna por banco). Se não, retorna None.
    """
    try:
        with get_conn(caminho_banco) as conn:
            df = pd.read_sql("SELECT * FROM saldos_bancos", conn)
        if df is None or df.empty:
            return None

        # Descobrir a coluna de data
        date_col = None
        for c in df.columns:
            if c.lower() in ("data", "date"):
                date_col = c
                break
        if date_col is not None:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)
            df = df[df[date_col].dt.date <= pd.to_datetime(data_str).date()]

        # Wide-table: soma da coluna do banco
        if banco_nome in df.columns:
            return float(pd.to_numeric(df[banco_nome], errors="coerce").fillna(0.0).sum())
    except Exception:
        return None
    return None


def _decrementar_saldos_bancos(caminho_banco: str, data_str: str, banco_nome: str, valor: float) -> None:
    """
    Decrementa `valor` na coluna do banco `banco_nome` para a linha da data `data_str`.
    Espelha a lógica de `upsert_saldos_bancos`, mas subtraindo.
    Ignora se valor <= 0.
    """
    if not valor or valor <= 0:
        return

    import sqlite3
    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # Garante existência da coluna do banco
        cols_info = cur.execute("PRAGMA table_info(saldos_bancos);").fetchall()
        existentes = {c[1] for c in cols_info}
        if banco_nome not in existentes:
            cur.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_nome}" REAL NOT NULL DEFAULT 0.0')
            conn.commit()
            cols_info = cur.execute("PRAGMA table_info(saldos_bancos);").fetchall()
            existentes = {c[1] for c in cols_info}

        # Descobre coluna de data
        def _date_col_name():
            cols = [r[1] for r in cols_info]
            for cand in ("data", "Data"):
                if cand in cols:
                    return cand
            return "data"

        date_col = _date_col_name()

        # Tenta atualizar linha existente, senão insere uma nova com o débito
        row = cur.execute(f'SELECT rowid FROM saldos_bancos WHERE "{date_col}"=? LIMIT 1;', (data_str,)).fetchone()
        if row:
            cur.execute(
                f'UPDATE saldos_bancos SET "{banco_nome}" = COALESCE("{banco_nome}", 0.0) - ? '
                f'WHERE "{date_col}" = ?;',
                (float(valor), data_str)
            )
        else:
            # Insere a linha do dia com débito dessa conta (demais colunas = 0)
            colnames = [c[1] for c in cols_info]
            outras = [c for c in colnames if c != date_col]
            placeholders = ",".join(["?"] * (1 + len(outras)))
            cols_sql = f'"{date_col}",' + ",".join(f'"{c}"' for c in outras)
            valores = [data_str] + [0.0] * len(outras)
            if banco_nome in outras:
                idx = 1 + outras.index(banco_nome)
                valores[idx] = -float(valor)
            cur.execute(f'INSERT INTO saldos_bancos ({cols_sql}) VALUES ({placeholders});', valores)

        conn.commit()


def registrar_transferencia_bancaria(
    caminho_banco: str,
    data_lanc,
    banco_origem_in: str,
    banco_destino_in: str,
    valor: float,
    observacao: str | None = None,
) -> ResultadoTransferenciaBancos:
    """
    Registra a transferência banco→banco:
    - validações
    - duas linhas em movimentacoes_bancarias (saida/entrada)
    - ajuste em saldos_bancos (— origem, + destino)
    """
    # --- validações básicas ---
    try:
        valor_f = float(valor)
    except Exception:
        valor_f = 0.0
    if valor_f <= 0:
        raise ValueError("Valor inválido.")
    if not (banco_origem_in and banco_destino_in):
        raise ValueError("Informe banco de origem e destino.")
    if str(banco_origem_in).strip().lower() == str(banco_destino_in).strip().lower():
        raise ValueError("Origem e destino não podem ser o mesmo banco.")

    # Canonicaliza nomes
    try:
        banco_origem = canonicalizar_banco(caminho_banco, banco_origem_in) or (banco_origem_in or "").strip()
    except Exception:
        banco_origem = (banco_origem_in or "").strip()

    try:
        banco_destino = canonicalizar_banco(caminho_banco, banco_destino_in) or (banco_destino_in or "").strip()
    except Exception:
        banco_destino = (banco_destino_in or "").strip()

    # --- normaliza data ---
    data_dt: _date = coerce_data(data_lanc)  # aceita None/str/date
    data_str = data_dt.strftime("%Y-%m-%d")

    # (Opcional) checar saldo do banco origem (best-effort; aqui usamos warning via ValueError)
    saldo_origem = _try_saldo_banco(caminho_banco, banco_origem, data_str)
    if saldo_origem is not None and valor_f > saldo_origem:
        raise ValueError(
            f"Saldo insuficiente no banco '{banco_origem}'. "
            f"Disponível até {data_str}: R$ {saldo_origem:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        )

    # --- grava duas movimentações (saida/entrada), respeitando CHECK(tipo) ---
    repo = MovimentacoesRepository(caminho_banco)
    trans_uid = str(uuid.uuid4())
    obs_extra = f" | {observacao.strip()}" if (observacao and observacao.strip()) else ""

    # SAÍDA - origem
    repo.registrar_saida(
        data=data_str,
        banco=banco_origem,
        valor=valor_f,
        origem="transferencia",
        observacao=f"Transferência para {banco_destino} | TX={trans_uid}{obs_extra}",
        referencia_tabela="transferencias",
        referencia_id=None,
    )

    # ENTRADA - destino
    repo.registrar_entrada(
        data=data_str,
        banco=banco_destino,
        valor=valor_f,
        origem="transferencia",
        observacao=f"Transferência de {banco_origem} | TX={trans_uid}{obs_extra}",
        referencia_tabela="transferencias",
        referencia_id=None,
    )

    # --- ajustes em saldos_bancos ---
    _decrementar_saldos_bancos(caminho_banco, data_str, banco_origem, valor_f)   # — origem
    upsert_saldos_bancos(caminho_banco, data_str, banco_destino, valor_f)        # + destino

    return {
        "ok": True,
        "msg": (
            f"✅ Transferência registrada: {banco_origem} → {banco_destino} "
            f"no valor de R$ {valor_f:,.2f} em {data_dt.strftime('%d/%m/%Y')}"
        ).replace(",", "X").replace(".", ",").replace("X", "."),
        "origem": banco_origem,
        "destino": banco_destino,
        "valor": valor_f,
    }
