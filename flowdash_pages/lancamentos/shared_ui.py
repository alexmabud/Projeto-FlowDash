"""
Módulo Shared UI
================

Componentes reutilizáveis de interface e alguns helpers de banco/negócio
compartilhados entre as páginas de lançamentos.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Optional, Any
from datetime import date, timedelta

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from html import escape

# Imports internos do FlowDash
# Preferir o pacote `utils` (que já faz alias se preciso); cair para utils.utils se necessário.
try:
    from utils import formatar_valor
except Exception:
    try:
        from utils.utils import formatar_valor  # compatibilidade antiga
    except Exception:
        # Fallback defensivo: formata como BRL
        def formatar_valor(v):
            try:
                n = float(v or 0.0)
            except Exception:
                n = 0.0
            return f"R$ {n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

from shared.db import get_conn
from shared.ids import uid_venda_liquidacao
from repository.movimentacoes_repository import MovimentacoesRepository


# ===========================
# Helpers de DataFrames / UI
# ===========================
_TBL_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')  # whitelist p/ nomes de tabela

def _validate_table_name(nome_tabela: str) -> str:
    """Valida e retorna o nome de tabela seguro (whitelist)."""
    nt = (nome_tabela or '').strip()
    if not _TBL_RE.match(nt):
        raise ValueError(f"Nome de tabela inválido: {nome_tabela!r}")
    return nt

def carregar_tabela(nome_tabela: str, caminho_banco: str) -> pd.DataFrame:
    """
    Carrega uma tabela do banco em DataFrame, converte coluna de data e
    padroniza seu nome para 'data' (aceita 'data' ou 'Data').

    Observação:
        Usa `dayfirst=True` no parsing de datas para aderir ao padrão BR.
    """
    try:
        nt = _validate_table_name(nome_tabela)
        with get_conn(caminho_banco) as conn:
            df = pd.read_sql(f'SELECT * FROM "{nt}"', conn)

        # Detecta coluna de data, qualquer variação de caixa
        col_data = next((c for c in df.columns if c.lower() == "data"), None)
        if col_data:
            df[col_data] = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True)
            if col_data != "data":
                df = df.rename(columns={col_data: "data"})
        return df
    except Exception:
        return pd.DataFrame()

def bloco_resumo_dia(itens_ou_linhas, titulo: str = "📆 Resumo do Dia"):
    """
    Renderiza um cartão de resumo como HTML real (components.html).

    Aceita:
        - lista plana: [("Label", "Valor"), ...]              -> 1 linha
        - lista de linhas: [[("Label","Valor"), ...], ...]    -> n linhas
    """
    # Normaliza para lista de linhas
    if itens_ou_linhas and isinstance(itens_ou_linhas[0], tuple):
        linhas = [itens_ou_linhas]
    else:
        linhas = itens_ou_linhas or []

    if not linhas:
        components.html(
            f'<div style="border:1px solid #444; border-radius:10px; padding:18px 16px; background:#1c1c1c; margin:10px 0 20px;">'
            f'  <h4 style="color:#fff; margin:0 0 14px; font-size:1.15rem;">{escape(titulo)}</h4>'
            f'  <div style="color:#aaa;">Sem dados para exibir.</div>'
            f'</div>',
            height=150, scrolling=False
        )
        return

    linhas_html = []
    for linha in linhas:
        if not linha:
            continue
        width = f"{100 / max(1, len(linha)):.6f}%"
        tds = []
        for k, v in linha:
            # formatação e escape
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                v_fmt = formatar_valor(float(v))
            else:
                v_fmt = str(v)
            k_html = escape(str(k))
            v_html = escape(str(v_fmt))
            tds.append(
                f'<td style="text-align:center; width:{width}; padding:8px 6px; vertical-align:top;">'
                f'  <div style="color:#ccc; font-weight:600; font-size:0.92rem; line-height:1.2; word-break:break-word;">{k_html}</div>'
                f'  <div style="font-size:1.35rem; color:#00FFAA; font-weight:700; margin-top:4px;">{v_html}</div>'
                f'</td>'
            )
        linhas_html.append(f"<tr>{''.join(tds)}</tr>")

    html = (
        f'<div style="border:1px solid #444; border-radius:10px; padding:18px 16px; background:#1c1c1c; margin:10px 0 20px;">'
        f'  <h4 style="color:#fff; margin:0 0 14px; font-size:1.15rem;">{escape(titulo)}</h4>'
        f'  <table style="width:100%; border-collapse:collapse; table-layout:fixed;">'
        f'    {"".join(linhas_html)}'
        f'  </table>'
        f'</div>'
    )

    # Altura estimada (base + ~64px por linha)
    linhas_count = sum(1 for l in linhas if l)
    height = max(160, 120 + 64 * linhas_count)
    components.html(html, height=height, scrolling=False)


# ===========================
# Regras usadas em venda
# ===========================
DIAS_COMPENSACAO = {
    "DINHEIRO": 0,
    "PIX": 0,
    "DÉBITO": 1,
    "CRÉDITO": 1,
    "LINK_PAGAMENTO": 1,
}

def proximo_dia_util_br(data_base: date, dias: int) -> date:
    """
    Retorna a próxima data útil no Brasil (considera fins de semana e, se possível, feriados).

    Fallback:
        Se a biblioteca de feriados não estiver disponível, considera apenas fins de semana.
    """
    try:
        from workalendar.america import BrazilDistritoFederal
        cal = BrazilDistritoFederal()
        d, add = data_base, 0
        while add < dias:
            d += timedelta(days=1)
            if cal.is_working_day(d):
                add += 1
        return d
    except Exception:
        # fallback: considera apenas fins de semana
        d, add = data_base, 0
        while add < dias:
            d += timedelta(days=1)
            if d.weekday() < 5:
                add += 1
        return d

def inserir_mov_liquidacao_venda(
    caminho_banco: str,
    data_: str,
    banco: str,
    valor_liquido: float,
    observacao: str,
    referencia_id: Optional[int]
) -> None:
    """
    Registra a liquidação da venda em movimentacoes_bancarias com idempotência:

    - tipo='entrada' / origem='vendas_liquidacao'
    - referencia_tabela='entrada'
    - trans_uid via `uid_venda_liquidacao`
    """
    if not valor_liquido or valor_liquido <= 0:
        return

    # neutros se UI não passar
    forma = "N/A"; maquineta = ""; bandeira = ""; parcelas = 1; usuario = "Sistema"

    nome_banco = canonicalizar_banco(caminho_banco, banco) or (banco or "").strip()

    trans_uid = uid_venda_liquidacao(
        data_liq=str(data_),
        valor_liq=float(valor_liquido),
        forma=forma,
        maquineta=maquineta,
        bandeira=bandeira,
        parcelas=parcelas,
        banco=nome_banco,
        usuario=usuario
    )

    mov_repo = MovimentacoesRepository(caminho_banco)
    mov_repo.registrar_entrada(
        data=str(data_),
        banco=nome_banco,
        valor=float(valor_liquido),
        origem="vendas_liquidacao",
        observacao=observacao or "",
        referencia_tabela="entrada",
        referencia_id=int(referencia_id) if referencia_id else None,
        trans_uid=trans_uid
    )

def registrar_caixa_vendas(caminho_banco: str, data_: str, valor: float) -> None:
    """Atualiza o saldo de `caixa_vendas` em `saldos_caixas` (soma na mesma data)."""
    if not valor or valor <= 0:
        return
    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE saldos_caixas SET caixa_vendas = COALESCE(caixa_vendas,0)+? WHERE data=?",
                (float(valor), data_)
            )
            if cur.rowcount == 0:
                cur.execute(
                    "INSERT INTO saldos_caixas (data, caixa_vendas) VALUES (?, ?)",
                    (data_, float(valor))
                )
        except sqlite3.OperationalError:
            cur.execute(
                "UPDATE saldos_caixas SET caixa_vendas = COALESCE(caixa_vendas,0)+? WHERE Data=?",
                (float(valor), data_)
            )
            if cur.rowcount == 0:
                cur.execute(
                    "INSERT INTO saldos_caixas (Data, caixa_vendas) VALUES (?, ?)",
                    (data_, float(valor))
                )
        conn.commit()

def obter_banco_destino(
    caminho_banco: str,
    forma: str,
    maquineta: str,
    bandeira: Optional[str],
    parcelas: Optional[int]
) -> Optional[str]:
    """
    Obtém banco destino (tabela `taxas_maquinas`) de acordo com forma, maquineta, bandeira e parcelas.

    Notas:
        - Matching **case-insensitive** para `forma_pagamento` via `UPPER(...)`.
        - Tenta variação para LINK_PAGAMENTO utilizando CRÉDITO como fallback.
    """
    formas_try = [forma]
    if forma == "LINK_PAGAMENTO":
        formas_try.append("CRÉDITO")

    with get_conn(caminho_banco) as conn:
        # match preciso por bandeira e parcelas (case-insensitive na forma)
        for f in formas_try:
            row = conn.execute(
                """
                SELECT banco_destino FROM taxas_maquinas
                WHERE UPPER(forma_pagamento)=? AND maquineta=? AND bandeira=? AND parcelas=?
                LIMIT 1
                """,
                (f.upper(), maquineta or "", bandeira or "", int(parcelas or 1))
            ).fetchone()
            if row and row[0]:
                return row[0]

        # fallback por maquineta (sem filtrar bandeira/parcelas), ainda case-insensitive na forma
        for f in formas_try:
            row = conn.execute(
                """
                SELECT banco_destino FROM taxas_maquinas
                WHERE UPPER(forma_pagamento)=? AND maquineta=?
                  AND banco_destino IS NOT NULL AND TRIM(banco_destino)<>'' 
                LIMIT 1
                """,
                (f.upper(), maquineta or "")
            ).fetchone()
            if row and row[0]:
                return row[0]

        # último fallback: qualquer registro da maquineta com banco_destino definido
        row = conn.execute(
            """
            SELECT banco_destino FROM taxas_maquinas
            WHERE maquineta=? AND banco_destino IS NOT NULL AND TRIM(banco_destino)<>'' 
            LIMIT 1
            """,
            (maquineta or "",)
        ).fetchone()
        if row and row[0]:
            return row[0]

    return None


# ==========================================
# Helpers para evitar colunas erradas (Teste)
# e somar no saldos_bancos na mesma data
# ==========================================
def _normalize_bank(s: str) -> str:
    """Normaliza nome de banco (A-Z0-9)."""
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())

def canonicalizar_banco(caminho_banco: str, nome_banco: str) -> Optional[str]:
    """
    Retorna o nome EXATO (como cadastrado) em `bancos_cadastrados` para o `nome_banco` informado.
    Evita criar colunas erradas em `saldos_bancos`.
    """
    alvo = _normalize_bank(nome_banco)
    with get_conn(caminho_banco) as conn:
        try:
            df = pd.read_sql("SELECT nome FROM bancos_cadastrados", conn)
            nomes = df["nome"].dropna().astype(str).tolist()
        except Exception:
            nomes = []
    for n in nomes:
        if _normalize_bank(n) == alvo:
            return n
    aliases = {
        "INFINITEPAY": "InfinitePay",
        "INFINITYPAY": "InfinitePay",
        "INFINITEPAYBRASIL": "InfinitePay",
        "BANCOINTER": "Inter",
        "INTER": "Inter",
        "BRADESCO": "Bradesco",
    }
    if alvo in aliases and aliases[alvo] in nomes:
        return aliases[alvo]
    return None

def _date_col_name(conn: sqlite3.Connection, table: str) -> str:
    """Descobre o nome da coluna de data ('data' ou 'Data') em uma tabela (com validação do nome)."""
    table_safe = _validate_table_name(table)
    cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{table_safe}");').fetchall()]
    for cand in ("data", "Data"):
        if cand in cols:
            return cand
    return "data"  # fallback

def upsert_saldos_bancos(caminho_banco: str, data_str: str, banco_nome: str, valor: float) -> None:
    """
    Soma `valor` na coluna do banco `banco_nome` na linha da data `data_str`.

    Regras:
        - Garante que a coluna exista (`REAL NOT NULL DEFAULT 0.0`).
        - Cria a linha da data se necessário (ou soma se já existir).
        - Usa COALESCE para evitar `NULL`.
        - Funciona com coluna `data` OU `Data`.
    """
    if not valor or valor <= 0:
        return

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        try:
            nomes_cadastrados = pd.read_sql("SELECT nome FROM bancos_cadastrados", conn)["nome"].astype(str).tolist()
        except Exception:
            nomes_cadastrados = []
        if banco_nome not in nomes_cadastrados:
            raise ValueError(f"Banco '{banco_nome}' não está registrado em bancos_cadastrados.")

        cols_info = cur.execute('PRAGMA table_info("saldos_bancos");').fetchall()
        existentes = {c[1] for c in cols_info}
        if banco_nome not in existentes:
            cur.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_nome}" REAL NOT NULL DEFAULT 0.0')
            conn.commit()
            cols_info = cur.execute('PRAGMA table_info("saldos_bancos");').fetchall()
            existentes = {c[1] for c in cols_info}

        date_col = _date_col_name(conn, "saldos_bancos")

        row = cur.execute(f'SELECT rowid FROM saldos_bancos WHERE "{date_col}"=? LIMIT 1;', (data_str,)).fetchone()
        if row:
            cur.execute(
                f'UPDATE saldos_bancos SET "{banco_nome}" = COALESCE("{banco_nome}", 0.0) + ? '
                f'WHERE "{date_col}" = ?;',
                (float(valor), data_str)
            )
        else:
            colnames = [c[1] for c in cols_info]
            outras = [c for c in colnames if c != date_col]
            placeholders = ",".join(["?"] * (1 + len(outras)))
            cols_sql = f'"{date_col}",' + ",".join(f'"{c}"' for c in outras)
            valores = [data_str] + [0.0] * len(outras)
            if banco_nome in outras:
                valores[1 + outras.index(banco_nome)] = float(valor)
            else:
                raise RuntimeError(f"Coluna '{banco_nome}' não encontrada após criação em saldos_bancos.")
            cur.execute(f'INSERT INTO saldos_bancos ({cols_sql}) VALUES ({placeholders});', valores)

        conn.commit()


# ===========================
# API pública (estável)
# ===========================
__all__ = [
    "carregar_tabela",
    "DIAS_COMPENSACAO", "proximo_dia_util_br",
    "inserir_mov_liquidacao_venda", "registrar_caixa_vendas", "obter_banco_destino",
    "canonicalizar_banco", "upsert_saldos_bancos",
    "bloco_resumo_dia",
    "formatar_valor",
]
