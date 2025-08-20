from __future__ import annotations
import pandas as pd

def r2(x) -> float:
    """Arredonda em 2 casas (evita -0,00) mantendo compatibilidade com os _r2 atuais."""
    try:
        return round(float(x or 0.0), 2)
    except Exception:
        return 0.0

def date_col_name(df_or_conn, table: str | None = None) -> str:
    """
    Descobre o nome da coluna de data em um DataFrame OU via PRAGMA num conn.
    Prioriza 'data' e 'Data'. Fallback: 'data'.
    """
    if isinstance(df_or_conn, pd.DataFrame):
        cols = set(map(str, df_or_conn.columns))
    else:
        # df_or_conn é uma conexão sqlite3
        cur = df_or_conn.cursor()
        cols = {r[1] for r in cur.execute(f"PRAGMA table_info({table});").fetchall()}

    for cand in ("data", "Data"):
        if cand in cols:
            return cand
    return "data"

def coerce_date_col(df: pd.DataFrame, *candidates: str) -> str | None:
    """
    Converte a primeira coluna candidata para datetime (dayfirst=True) e retorna o nome.
    Se não encontrar, retorna None.
    """
    if df is None or df.empty:
        return None
    candidates = candidates or ("data", "Data")
    low = {c.lower(): c for c in df.columns}
    for c in candidates:
        real = low.get(c.lower())
        if real:
            try:
                df[real] = pd.to_datetime(df[real], errors="coerce", dayfirst=True)
            finally:
                return real
    return None