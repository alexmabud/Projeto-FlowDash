# ===================== Actions: Transferência =====================
"""
Executa a lógica da **transferência banco → banco**.

Fluxo:
    1) Canonicaliza nomes dos bancos (consistentes com `saldos_bancos`).
    2) Registra **duas linhas** em `movimentacoes_bancarias`:
        - SAÍDA no banco de ORIGEM  (tipo='saida',   origem='transferencia')
        - ENTRADA no banco de DESTINO (tipo='entrada', origem='transferencia')
       As linhas ficam pareadas por `referencia_id` (cross-link).
    3) Atualiza `saldos_bancos` no dia:
        - decrementa coluna do banco de ORIGEM
        - incrementa coluna do banco de DESTINO

Observação:
    - O texto salvo em `observacao` segue o padrão **sem TX**:
        * Saída  : "Lançamento TRANSFERÊNCIA para {BancoDestino} | Valor R$X,XX"
        * Entrada: "Lançamento TRANSFERÊNCIA de {BancoOrigem} | Valor R$X,XX"
"""

from __future__ import annotations

from datetime import date as _date, datetime as _dt
from typing import List, Optional, TypedDict

import pandas as pd

from repository.movimentacoes_repository import MovimentacoesRepository
from shared.db import get_conn
from utils.utils import coerce_data, formatar_moeda
from flowdash_pages.cadastros.cadastro_classes import BancoRepository
from flowdash_pages.lancamentos.shared_ui import canonicalizar_banco, upsert_saldos_bancos


# ===================== Tipos =====================
class ResultadoTransferenciaBancos(TypedDict):
    """Payload de retorno de `registrar_transferencia_bancaria`."""
    ok: bool
    msg: str
    origem: str
    destino: str
    valor: float


# ===================== Helpers =====================
def carregar_nomes_bancos(caminho_banco: str) -> List[str]:
    """Retorna a lista de bancos cadastrados (mesmo comportamento do Depósito).

    Args:
        caminho_banco: Caminho do arquivo SQLite.

    Returns:
        Lista com os nomes dos bancos (pode ser vazia).
    """
    repo = BancoRepository(caminho_banco)
    df = repo.carregar_bancos()
    return df["nome"].tolist() if df is not None and not df.empty else []


def _try_saldo_banco(caminho_banco: str, banco_nome: str, data_str: str) -> Optional[float]:
    """Calcula o saldo acumulado (≤ data) de um banco em `saldos_bancos`.

    Usa a tabela wide `saldos_bancos`, somando os valores da coluna do banco
    até a data informada (inclusive). Se algo falhar, retorna `None`.

    Args:
        caminho_banco: Caminho do arquivo SQLite.
        banco_nome: Nome (coluna) do banco na tabela.
        data_str: Data no formato "YYYY-MM-DD".

    Returns:
        Saldo acumulado (float) ou `None` se não for possível calcular.
    """
    try:
        with get_conn(caminho_banco) as conn:
            df = pd.read_sql("SELECT * FROM saldos_bancos", conn)
        if df is None or df.empty:
            return None

        # Detecta coluna de data
        date_col = next((c for c in df.columns if c.lower() in ("data", "date")), None)
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            limite = pd.to_datetime(data_str, errors="coerce")
            if pd.isna(limte := limite):
                return None
            df = df[df[date_col] <= limte]

        if banco_nome in df.columns:
            return float(pd.to_numeric(df[banco_nome], errors="coerce").fillna(0.0).sum())
    except Exception:
        return None
    return None


def _ensure_cols_movs(caminho_banco: str) -> None:
    """Garante colunas extras em `movimentacoes_bancarias`.

    Cria, se ausentes:
        - `usuario` (TEXT)
        - `data_hora` (TEXT)
        - `referencia_id` (INTEGER NULL)
    """
    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()
        info = cur.execute("PRAGMA table_info(movimentacoes_bancarias);").fetchall()
        cols = {r[1] for r in info}

        if "usuario" not in cols:
            cur.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "usuario" TEXT;')
        if "data_hora" not in cols:
            cur.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "data_hora" TEXT;')
        if "referencia_id" not in cols:
            cur.execute('ALTER TABLE movimentacoes_bancarias ADD COLUMN "referencia_id" INTEGER;')
        conn.commit()


def _decrementar_saldos_bancos(caminho_banco: str, data_str: str, banco_nome: str, valor: float) -> None:
    """Decrementa `valor` no banco `banco_nome` em `saldos_bancos` na data `data_str`.

    Args:
        caminho_banco: Caminho do arquivo SQLite.
        data_str: Data em "YYYY-MM-DD".
        banco_nome: Coluna do banco.
        valor: Valor a decrementar (ignorado se ≤ 0).
    """
    if not valor or valor <= 0:
        return

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # Garante coluna do banco
        cols_info = cur.execute("PRAGMA table_info(saldos_bancos);").fetchall()
        existentes = {c[1] for c in cols_info}
        if banco_nome not in existentes:
            cur.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_nome}" REAL NOT NULL DEFAULT 0.0')
            conn.commit()
            cols_info = cur.execute("PRAGMA table_info(saldos_bancos);").fetchall()

        # Coluna de data
        cols = [r[1] for r in cols_info]
        date_col = "data" if "data" in cols else ("Data" if "Data" in cols else "data")

        # Atualiza a linha do dia ou insere nova
        row = cur.execute(
            f'SELECT rowid FROM saldos_bancos WHERE "{date_col}"=? LIMIT 1;',
            (data_str,),
        ).fetchone()

        if row:
            cur.execute(
                f'UPDATE saldos_bancos '
                f'SET "{banco_nome}" = COALESCE("{banco_nome}", 0.0) - ? '
                f'WHERE "{date_col}" = ?;',
                (float(valor), data_str),
            )
        else:
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


# ===================== Ação principal =====================
def registrar_transferencia_bancaria(
    caminho_banco: str,
    data_lanc,
    banco_origem_in: str,
    banco_destino_in: str,
    valor: float,
    usuario: Optional[str] = None,
    observacao: Optional[str] = None,  # ignorado no texto principal (pedido "só isso")
) -> ResultadoTransferenciaBancos:
    """Registra uma transferência banco→banco.

    Etapas:
        - Validações de entrada.
        - Escrita de 2 linhas em `movimentacoes_bancarias` (saida/entrada), com:
          `referencia_id` (cross), `usuario` e `data_hora`.
        - Ajuste em `saldos_bancos` (— origem, + destino).

    Args:
        caminho_banco: Caminho do arquivo SQLite.
        data_lanc: Data do lançamento (aceita date/datetime/str/None).
        banco_origem_in: Nome do banco de origem (livre; será canonicalizado).
        banco_destino_in: Nome do banco de destino (livre; será canonicalizado).
        valor: Valor da transferência.
        usuario: Nome do usuário executor (fallback: session_state → "sistema").
        observacao: Texto adicional (não incluído no texto principal).

    Returns:
        Estrutura `ResultadoTransferenciaBancos` com status e mensagem.

    Raises:
        ValueError: Se alguma validação falhar (valor inválido, bancos ausentes/iguais,
                    saldo insuficiente quando checável).
    """
    # --- resolve usuário (preferir NOME em session_state) ---
    if not usuario:
        try:
            import streamlit as st
            u = st.session_state.get("usuario_logado")
            if isinstance(u, dict):
                usuario = str(u.get("nome") or "").strip()
            elif isinstance(u, str):
                usuario = u.strip()
        except Exception:
            pass
    if not usuario:
        usuario = "sistema"

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

    # Canonicaliza nomes (mantém o input em caso de falha)
    try:
        banco_origem = canonicalizar_banco(caminho_banco, banco_origem_in) or (banco_origem_in or "").strip()
    except Exception:
        banco_origem = (banco_origem_in or "").strip()
    try:
        banco_destino = canonicalizar_banco(caminho_banco, banco_destino_in) or (banco_destino_in or "").strip()
    except Exception:
        banco_destino = (banco_destino_in or "").strip()

    # --- normaliza data/hora ---
    data_dt: _date = coerce_data(data_lanc)
    data_str = data_dt.strftime("%Y-%m-%d")
    data_hora = _dt.now().strftime("%Y-%m-%d %H:%M:%S")

    # Garante colunas extras em `movimentacoes_bancarias`
    _ensure_cols_movs(caminho_banco)

    # (Opcional) checar saldo do banco origem
    saldo_origem = _try_saldo_banco(caminho_banco, banco_origem, data_str)
    if saldo_origem is not None and valor_f > saldo_origem:
        raise ValueError(
            f"Saldo insuficiente no banco '{banco_origem}'. Disponível até {data_str}: {formatar_moeda(saldo_origem)}"
        )

    # --- grava duas movimentações (saida/entrada) ---
    repo = MovimentacoesRepository(caminho_banco)

    # Texto EXATO (sem TX/UID) — pedido
    valor_fmt = formatar_moeda(valor_f)
    obs_saida = f"Lançamento TRANSFERÊNCIA para {banco_destino} | Valor {valor_fmt}"
    obs_entrada = f"Lançamento TRANSFERÊNCIA de {banco_origem} | Valor {valor_fmt}"

    # SAÍDA (origem) — primeiro, para obter `id_saida`
    id_saida = repo.registrar_saida(
        data=data_str,
        banco=banco_origem,
        valor=valor_f,
        origem="transferencia",
        observacao=obs_saida,
        referencia_tabela="transferencias",
        referencia_id=None,  # cross-set depois
    )

    # ENTRADA (destino) — referencia a SAÍDA
    id_entrada = repo.registrar_entrada(
        data=data_str,
        banco=banco_destino,
        valor=valor_f,
        origem="transferencia",
        observacao=obs_entrada,
        referencia_tabela="transferencias",
        referencia_id=id_saida,
    )

    # Atualizações extras: usuario/data_hora + cross referencia_id
    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE movimentacoes_bancarias SET usuario=?, data_hora=? WHERE id=?;",
            (usuario, data_hora, id_saida),
        )
        cur.execute(
            "UPDATE movimentacoes_bancarias SET usuario=?, data_hora=? WHERE id=?;",
            (usuario, data_hora, id_entrada),
        )
        cur.execute(
            "UPDATE movimentacoes_bancarias SET referencia_id=? WHERE id=?;",
            (id_entrada, id_saida),
        )
        conn.commit()

    # --- ajustes em saldos_bancos ---
    _decrementar_saldos_bancos(caminho_banco, data_str, banco_origem, valor_f)  # — origem
    upsert_saldos_bancos(caminho_banco, data_str, banco_destino, valor_f)       # + destino

    return {
        "ok": True,
        "msg": (
            f"✅ Transferência registrada: {banco_origem} → {banco_destino} "
            f"no valor de {valor_fmt} em {data_dt.strftime('%d/%m/%Y')}"
        ),
        "origem": banco_origem,
        "destino": banco_destino,
        "valor": valor_f,
    }
