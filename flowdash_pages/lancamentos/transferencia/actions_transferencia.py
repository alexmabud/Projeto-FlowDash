# ===================== Actions: Transferência =====================
"""
Executa a MESMA lógica/SQL do módulo original de Transferência Banco → Banco:

- movimentacoes_bancarias: cria 2 linhas
    * SAÍDA no banco de origem (origem='transf_bancos_saida')
    * ENTRADA no banco de destino (origem='transf_bancos')
  (o card de resumo soma apenas origem='transf_bancos', então contará só a ENTRADA)

- saldos_bancos: soma no DESTINO (upsert_saldos_bancos) e subtrai na ORIGEM (_subtrair_saldo_banco)
"""

import uuid
from typing import TypedDict

import pandas as pd
from shared.db import get_conn
from utils.utils import formatar_valor
from flowdash_pages.cadastros.cadastro_classes import BancoRepository
from flowdash_pages.lancamentos.shared_ui import upsert_saldos_bancos, canonicalizar_banco

class ResultadoTransferencia(TypedDict):
    ok: bool
    msg: str
    banco_origem: str
    banco_destino: str
    valor: float
    saida_id: int
    entrada_id: int

def _r2(x) -> float:
    """Arredonda em 2 casas (evita -0,00)."""
    return round(float(x or 0.0), 2)

def carregar_nomes_bancos(caminho_banco: str) -> list[str]:
    """Obtém lista de nomes de bancos cadastrados."""
    repo = BancoRepository(caminho_banco)
    df = repo.carregar_bancos()
    return df["nome"].tolist() if df is not None and not df.empty else []

def _date_col_name(conn, table: str) -> str:
    """Descobre o nome da coluna de data ('data' ou 'Data')."""
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]
    for cand in ("data", "Data"):
        if cand in cols:
            return cand
    return "data"

def _subtrair_saldo_banco(caminho_banco: str, data_str: str, banco_nome: str, valor: float) -> None:
    """
    Subtrai 'valor' do saldo do banco na linha da 'data_str' em saldos_bancos.
    - Garante a coluna do banco.
    - Se a data não existir, cria a linha e subtrai nela.
    (Não usa upsert_saldos_bancos pois ela rejeita valores <= 0.)
    """
    if not valor or valor <= 0:
        return

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # Garante que o banco está cadastrado
        try:
            nomes_cadastrados = pd.read_sql("SELECT nome FROM bancos_cadastrados", conn)["nome"].astype(str).tolist()
        except Exception:
            nomes_cadastrados = []
        if banco_nome not in nomes_cadastrados:
            raise ValueError(f"Banco '{banco_nome}' não está cadastrado em bancos_cadastrados.")

        # Garante coluna do banco
        cols_info = cur.execute("PRAGMA table_info(saldos_bancos);").fetchall()
        existentes = {c[1] for c in cols_info}
        if banco_nome not in existentes:
            cur.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_nome}" REAL NOT NULL DEFAULT 0.0')
            conn.commit()
            cols_info = cur.execute("PRAGMA table_info(saldos_bancos);").fetchall()

        date_col = _date_col_name(conn, "saldos_bancos")

        # Se existe linha da data → UPDATE; senão → INSERT
        row = cur.execute(f'SELECT rowid FROM saldos_bancos WHERE "{date_col}"=? LIMIT 1;', (data_str,)).fetchone()
        if row:
            cur.execute(
                f'UPDATE saldos_bancos '
                f'SET "{banco_nome}" = COALESCE("{banco_nome}", 0.0) - ? '
                f'WHERE "{date_col}" = ?;',
                (float(valor), data_str)
            )
        else:
            # Monta um INSERT com -valor para o banco escolhido
            colnames = [c[1] for c in cols_info]  # inclui a coluna de data e as demais
            outras = [c for c in colnames if c != date_col]
            placeholders = ",".join(["?"] * (1 + len(outras)))
            cols_sql = f'"{date_col}",' + ",".join(f'"{c}"' for c in outras)
            valores = [data_str] + [0.0] * len(outras)
            if banco_nome in outras:
                valores[1 + outras.index(banco_nome)] = -float(valor)
            else:
                raise RuntimeError(f"Coluna '{banco_nome}' não encontrada após criação em saldos_bancos.")
            cur.execute(f'INSERT INTO saldos_bancos ({cols_sql}) VALUES ({placeholders});', valores)

        conn.commit()

def registrar_transferencia(
    caminho_banco: str,
    data_lanc,
    banco_origem_in: str,
    banco_destino_in: str,
    valor: float
) -> ResultadoTransferencia:
    """
    Registra a transferência de saldo entre dois bancos (banco → banco).

    Creates:
      - 2 linhas em movimentacoes_bancarias (SAÍDA/ENTRADA) compartilhando o mesmo referencia_id (id da SAÍDA).
      - Atualização em saldos_bancos (soma no destino, subtrai na origem).

    Raises:
        ValueError: para validações.
    """
    valor_f = _r2(valor)
    if valor_f <= 0:
        raise ValueError("Valor inválido.")

    b_origem_in = (banco_origem_in or "").strip()
    b_dest_in   = (banco_destino_in or "").strip()
    if not b_origem_in or not b_dest_in:
        raise ValueError("Informe banco de origem e banco de destino.")
    if b_origem_in.lower() == b_dest_in.lower():
        raise ValueError("Origem e destino não podem ser o mesmo banco.")

    # Canonicalizar nomes
    try:
        b_origem = canonicalizar_banco(caminho_banco, b_origem_in) or b_origem_in
    except Exception:
        b_origem = b_origem_in
    try:
        b_dest = canonicalizar_banco(caminho_banco, b_dest_in) or b_dest_in
    except Exception:
        b_dest = b_dest_in

    data_str = str(data_lanc)

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # 1) SAÍDA (inserimos primeiro para obter o id)
        saida_uid = str(uuid.uuid4())
        saida_obs = (
            f"Transferência: {b_origem} → {b_dest} | Saída | "
            f"Valor={formatar_valor(valor_f)}"
        )
        cur.execute("""
            INSERT INTO movimentacoes_bancarias
                (data, banco,  tipo,   valor,  origem,                observacao,
                 referencia_id, referencia_tabela, trans_uid)
            VALUES (?,   ?,     ?,      ?,      ?,                     ?,
                    ?,             ?,                 ?)
        """, (
            data_str, b_origem, "saida", valor_f, "transf_bancos_saida",
            saida_obs,
            None, "movimentacoes_bancarias", saida_uid
        ))
        saida_id = cur.lastrowid

        # 2) ENTRADA (mesmo referencia_id = id da SAÍDA)
        entrada_uid = str(uuid.uuid4())
        entrada_obs = (
            f"Transferência: {b_origem} → {b_dest} | Entrada | "
            f"Valor={formatar_valor(valor_f)} | REF={saida_id}"
        )
        cur.execute("""
            INSERT INTO movimentacoes_bancarias
                (data, banco,   tipo,     valor,  origem,         observacao,
                 referencia_id, referencia_tabela, trans_uid)
            VALUES (?,   ?,      ?,        ?,      ?,              ?,
                    ?,             ?,                 ?)
        """, (
            data_str, b_dest, "entrada", valor_f, "transf_bancos",
            entrada_obs,
            saida_id, "movimentacoes_bancarias", entrada_uid
        ))
        entrada_id = cur.lastrowid

        # 3) Atualiza a SAÍDA com referencia_id e REF
        saida_obs_final = (
            f"Transferência: {b_origem} → {b_dest} | Saída | "
            f"Valor={formatar_valor(valor_f)} | REF={saida_id}"
        )
        cur.execute("""
            UPDATE movimentacoes_bancarias
               SET referencia_id = ?,
                   referencia_tabela = ?,
                   observacao = ?
             WHERE id = ?
        """, (saida_id, "movimentacoes_bancarias", saida_obs_final, saida_id))

        conn.commit()

    # 4) Atualiza saldos_bancos (mesma data)
    try:
        upsert_saldos_bancos(caminho_banco, data_str, b_dest, valor_f)
    except Exception as e:
        # segue a mesma filosofia dos outros módulos: avisar/propagar sem quebrar toda operação
        raise RuntimeError(f"Não foi possível somar no destino '{b_dest}' em saldos_bancos: {e}") from e

    try:
        _subtrair_saldo_banco(caminho_banco, data_str, b_origem, valor_f)
    except Exception as e:
        raise RuntimeError(f"Não foi possível subtrair na origem '{b_origem}' em saldos_bancos: {e}") from e

    return {
        "ok": True,
        "msg": (
            f"✅ Transferência registrada: {formatar_valor(valor_f)} "
            f"de **{b_origem}** → **{b_dest}**. "
            f"(IDs: saída #{saida_id}, entrada #{entrada_id} • ref_id comum={saida_id})"
        ),
        "banco_origem": b_origem,
        "banco_destino": b_dest,
        "valor": valor_f,
        "saida_id": saida_id,
        "entrada_id": entrada_id,
    }