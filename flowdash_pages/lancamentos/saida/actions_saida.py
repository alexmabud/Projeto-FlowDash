# flowdash_pages/lancamentos/saida/actions_saida.py
"""
Actions: Saídas (Dinheiro e Bancária)

- Normaliza inputs e delega para `LedgerService.registrar_lancamento`.
- Integra CAP (BOLETO/FATURA_CARTAO/EMPRESTIMO).
- Mantém compatibilidade com funções/assinaturas antigas via aliases.

Retorno padrão das ações:
- dict: { ok: bool, id_saida: int|None, id_mov: int|None, mensagem: str|None }

Atualizações:
- Carga de categorias/subcategorias agora busca IDs reais em:
    • categorias_saida (id, nome)
    • subcategorias_saida (id, nome, categoria_id)
- Fornece provider: listar_subcategorias_por_categoria(categoria_id) -> list[dict]
- registrar_saida_action agora enriquece a descrição com "[Categoria / Subcategoria]"
  para refletir também em movimentacoes_bancarias.observacao.
"""

from __future__ import annotations

from typing import Optional, Dict, Any, Tuple, List, Iterable, Callable
from datetime import datetime
import contextlib

from services.ledger.service_ledger import LedgerService

# =============================================================================
# Constantes de UI / defaults
# =============================================================================
DEFAULT_FORMAS = ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "BOLETO"]
DEFAULT_ORIGENS = ["Caixa", "Caixa 2"]
DEFAULT_BANDEIRAS = ["VISA", "MASTERCARD", "ELO", "HIPERCARD", "AMEX"]
DEFAULT_BANCOS = ["Banco 1", "Banco 2", "Banco 3", "Banco 4"]


# =============================================================================
# Helpers de normalização
# =============================================================================
def _norm_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _money(v: Any) -> float:
    """Converte string BR/US em float. Heurística: último separador define decimal."""
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return 0.0
    dot, comma = s.rfind("."), s.rfind(",")
    try:
        if dot == -1 and comma == -1:
            return float(s)
        if dot > comma:
            return float(s.replace(",", ""))
        return float(s.replace(".", "").replace(",", "."))
    except Exception:
        with contextlib.suppress(Exception):
            return float(s.replace(",", "."))
        return 0.0


def _norm_date(s: Optional[str]) -> str:
    """YYYY-MM-DD. Aceita formatos comuns/ISO; se vazio/erro, usa hoje."""
    if not s:
        return datetime.now().strftime("%Y-%m-%d")
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"):
        with contextlib.suppress(Exception):
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
    with contextlib.suppress(Exception):
        return datetime.fromisoformat(s).date().strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")


def _norm_forma(forma: Optional[str]) -> str:
    f = (forma or "").strip().upper()
    if f == "DEBITO":
        return "DÉBITO"
    return f or "DINHEIRO"


def _resolve_obrigacao(
    *,
    tipo_obrigacao: Optional[str],
    obrigacao_id: Optional[int],
    obrigacao_id_fatura: Optional[int],
    obrigacao_id_boleto: Optional[int],
    obrigacao_id_emprestimo: Optional[int],
) -> Tuple[Optional[str], Optional[int]]:
    """Resolve (tipo_obrigacao, obrigacao_id) com suporte a campos legados."""
    t = _norm_str(tipo_obrigacao)

    oid: Optional[int] = None
    if isinstance(obrigacao_id, int):
        oid = obrigacao_id
    elif isinstance(obrigacao_id, str) and obrigacao_id.strip().isdigit():
        oid = int(obrigacao_id.strip())

    if t and oid:
        return t.upper(), oid
    if obrigacao_id_fatura is not None:
        return "FATURA_CARTAO", int(obrigacao_id_fatura)
    if obrigacao_id_boleto is not None:
        return "BOLETO", int(obrigacao_id_boleto)
    if obrigacao_id_emprestimo is not None:
        return "EMPRESTIMO", int(obrigacao_id_emprestimo)
    if t:
        return t.upper(), oid
    return None, None


# =============================================================================
# Ações principais
# =============================================================================
def registrar_saida_action(
    *,
    caminho_banco: str,
    valor: Any,
    forma: Optional[str] = None,                 # "DINHEIRO" | "PIX" | "DÉBITO"
    origem: Optional[str] = None,                # "Caixa"/"Caixa 2" (para DINHEIRO) ou nome da conta
    banco: Optional[str] = None,                 # nome do banco/conta quando bancária
    categoria: Optional[str] = None,
    subcategoria: Optional[str] = None,
    descricao: Optional[str] = None,
    usuario: Optional[str] = None,
    data: Optional[str] = None,
    juros: Any = 0.0,
    multa: Any = 0.0,
    desconto: Any = 0.0,
    trans_uid: Optional[str] = None,
    # Integração CAP (genérico + compat)
    tipo_obrigacao: Optional[str] = None,
    obrigacao_id: Optional[int] = None,
    obrigacao_id_fatura: Optional[int] = None,
    obrigacao_id_boleto: Optional[int] = None,
    obrigacao_id_emprestimo: Optional[int] = None,
) -> Dict[str, Any]:
    """Normaliza os dados e delega ao `LedgerService.registrar_lancamento`.

    Ajuste: a `descricao` é enriquecida com "[Categoria / Subcategoria]" para
    que:
      • saida.descricao armazene também a referência de classificação, e
      • movimentacoes_bancarias.observacao traga esse complemento junto
        com o que já aparece (o Ledger costuma usar essa mesma `descricao`).
    """
    valor_f = _money(valor)
    if valor_f <= 0:
        return {"ok": False, "mensagem": "Valor da saída deve ser maior que zero."}

    data_norm = _norm_date(data)
    forma_norm = _norm_forma(forma)
    tipo_obr, obr_id = _resolve_obrigacao(
        tipo_obrigacao=tipo_obrigacao,
        obrigacao_id=obrigacao_id,
        obrigacao_id_fatura=obrigacao_id_fatura,
        obrigacao_id_boleto=obrigacao_id_boleto,
        obrigacao_id_emprestimo=obrigacao_id_emprestimo,
    )

    # --- enriquecer descricao: "<descricao> [Categoria / Subcategoria]" ---
    cat_str = _norm_str(categoria)
    subcat_str = _norm_str(subcategoria)
    desc_user = _norm_str(descricao)
    sufixo_cat = None
    if cat_str or subcat_str:
        sufixo_cat = f"[{cat_str or '-'} / {subcat_str or '-'}]"

    if desc_user and sufixo_cat:
        descricao_enriquecida = f"{desc_user} {sufixo_cat}"
    elif desc_user:
        descricao_enriquecida = desc_user
    elif sufixo_cat:
        descricao_enriquecida = sufixo_cat
    else:
        descricao_enriquecida = None

    try:
        ledger = LedgerService(caminho_banco)
        id_saida, id_mov = ledger.registrar_lancamento(
            tipo_evento="SAIDA",
            # Estes dois vão para as colunas categoria/subcategoria da tabela `saida`
            categoria_evento=cat_str,
            subcategoria_evento=subcat_str,
            # Esta vai para saida.descricao e, tipicamente, para movimentacoes_bancarias.observacao
            descricao=descricao_enriquecida,
            # Demais campos
            valor_evento=valor_f,
            forma=forma_norm,
            origem=_norm_str(origem),
            banco=_norm_str(banco),
            juros=_money(juros),
            multa=_money(multa),
            desconto=_money(desconto),
            usuario=_norm_str(usuario) or "-",
            trans_uid=_norm_str(trans_uid),
            data_evento=data_norm,
            tipo_obrigacao=tipo_obr,
            obrigacao_id=obr_id,
        )
        return {"ok": True, "id_saida": id_saida, "id_mov": id_mov, "mensagem": None}
    except Exception as e:
        return {"ok": False, "mensagem": f"Falha ao registrar saída: {e}"}


def registrar_saida_dinheiro_action(
    *,
    caminho_banco: str,
    valor: Any,
    origem_dinheiro: Optional[str] = None,      # "Caixa" | "Caixa 2"
    categoria: Optional[str] = None,
    subcategoria: Optional[str] = None,
    descricao: Optional[str] = None,
    usuario: Optional[str] = None,
    data: Optional[str] = None,
    juros: Any = 0.0,
    multa: Any = 0.0,
    desconto: Any = 0.0,
    trans_uid: Optional[str] = None,
    # CAP
    tipo_obrigacao: Optional[str] = None,
    obrigacao_id: Optional[int] = None,
    obrigacao_id_fatura: Optional[int] = None,
    obrigacao_id_boleto: Optional[int] = None,
    obrigacao_id_emprestimo: Optional[int] = None,
) -> Dict[str, Any]:
    return registrar_saida_action(
        caminho_banco=caminho_banco,
        valor=valor,
        forma="DINHEIRO",
        origem=origem_dinheiro or "Caixa",
        categoria=categoria,
        subcategoria=subcategoria,
        descricao=descricao,
        usuario=usuario,
        data=data,
        juros=juros,
        multa=multa,
        desconto=desconto,
        trans_uid=trans_uid,
        tipo_obrigacao=tipo_obrigacao,
        obrigacao_id=obrigacao_id,
        obrigacao_id_fatura=obrigacao_id_fatura,
        obrigacao_id_boleto=obrigacao_id_boleto,
        obrigacao_id_emprestimo=obrigacao_id_emprestimo,
    )


def registrar_saida_bancaria_action(
    *,
    caminho_banco: str,
    valor: Any,
    forma: str,                                  # "PIX" | "DÉBITO" (aceita "DEBITO")
    banco_nome: Optional[str] = None,
    categoria: Optional[str] = None,
    subcategoria: Optional[str] = None,
    descricao: Optional[str] = None,
    usuario: Optional[str] = None,
    data: Optional[str] = None,
    juros: Any = 0.0,
    multa: Any = 0.0,
    desconto: Any = 0.0,
    trans_uid: Optional[str] = None,
    # CAP
    tipo_obrigacao: Optional[str] = None,
    obrigacao_id: Optional[int] = None,
    obrigacao_id_fatura: Optional[int] = None,
    obrigacao_id_boleto: Optional[int] = None,
    obrigacao_id_emprestimo: Optional[int] = None,
) -> Dict[str, Any]:
    return registrar_saida_action(
        caminho_banco=caminho_banco,
        valor=valor,
        forma=forma,
        banco=banco_nome,
        categoria=categoria,
        subcategoria=subcategoria,
        descricao=descricao,
        usuario=usuario,
        data=data,
        juros=juros,
        multa=multa,
        desconto=desconto,
        trans_uid=trans_uid,
        tipo_obrigacao=tipo_obrigacao,
        obrigacao_id=obrigacao_id,
        obrigacao_id_fatura=obrigacao_id_fatura,
        obrigacao_id_boleto=obrigacao_id_boleto,
        obrigacao_id_emprestimo=obrigacao_id_emprestimo,
    )


# =============================================================================
# SQLite utilitários para a UI (listas)
# =============================================================================
try:
    import sqlite3
except Exception:  # pragma: no cover
    sqlite3 = None  # permite rodar sem sqlite em ambientes de teste


def _open_sqlite(db_path: str):
    conn = sqlite3.connect(db_path, timeout=30)
    with contextlib.suppress(Exception):
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _fetch_names(conn, sql: str) -> List[str]:
    """Executa SELECT de uma coluna e devolve lista de strings limpas."""
    with contextlib.suppress(Exception):
        cur = conn.execute(sql)
        rows = cur.fetchall()
        out: List[str] = []
        for r in rows:
            try:
                out.append(str(r[0]).strip())
            except Exception:
                with contextlib.suppress(Exception):
                    keys = list(r.keys())  # sqlite3.Row
                    if keys:
                        out.append(str(r[keys[0]]).strip())
        return [x for x in out if x]
    return []


def _fetch_dicts(conn, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    """Executa SELECT e devolve lista de dicts (coluna->valor)."""
    with contextlib.suppress(Exception):
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = {k: r[k] for k in r.keys()}
            out.append(d)
        # reset row_factory
        conn.row_factory = None
        return out
    return []


def _first_nonempty_from(conn, tables: Iterable[str], column: str = "nome") -> List[str]:
    """Retorna a primeira lista não-vazia de nomes dentre possíveis tabelas."""
    for t in tables:
        lst = _fetch_names(
            conn,
            f"""
            SELECT {column}
              FROM {t}
             WHERE COALESCE(TRIM({column}), '') <> ''
             ORDER BY LOWER(TRIM({column}))
            """,
        )
        if lst:
            return lst
    return []


def _try_get_bancos(conn) -> List[str]:
    # 1) Tabelas de cadastro
    lst = _first_nonempty_from(conn, ["cadastro_bancos", "bancos"])
    if lst:
        return lst

    # 2) Heurística via tabela de saldos: usa nomes das colunas como bancos
    with contextlib.suppress(Exception):
        cur = conn.execute("PRAGMA table_info('saldos_bancos')")
        colunas = [c[1] for c in cur.fetchall()]  # (cid, name, type, notnull, dflt, pk)
        ignorar = {"id", "data", "created_at", "updated_at"}
        candidatos = [c for c in colunas if c not in ignorar]
        if candidatos:
            return sorted(candidatos, key=lambda s: s.lower().strip())

    # 3) Fallback
    return DEFAULT_BANCOS[:]


def _try_get_cartoes(conn) -> List[str]:
    return _first_nonempty_from(conn, ["cartoes_credito", "cartoes"])


def _try_get_bandeiras(conn) -> List[str]:
    return _first_nonempty_from(conn, ["bandeiras_cartao", "cartoes_bandeiras", "bandeiras"]) or DEFAULT_BANDEIRAS[:]


def _get_categorias_full(conn) -> List[Dict[str, Any]]:
    """
    Retorna categorias com IDs reais.
    Preferência: categorias_saida(id, nome) -> fallback: categorias(nome) sem id.
    """
    cats = _fetch_dicts(
        conn,
        """
        SELECT id, nome
          FROM categorias_saida
         WHERE COALESCE(TRIM(nome), '') <> ''
         ORDER BY LOWER(TRIM(nome))
        """,
    )
    if cats:
        return cats
    # Fallback sem id (gera id sintético na page, se necessário)
    nomes = _first_nonempty_from(conn, ["categorias", "cadastro_categorias_saida"])
    return [{"id": idx + 1, "nome": n} for idx, n in enumerate(nomes)]


def _get_subcategorias_full(conn) -> List[Dict[str, Any]]:
    """
    Retorna subcategorias com IDs reais e vínculo por categoria_id.
    Preferência: subcategorias_saida(id, nome, categoria_id) -> fallback plano (sem vínculo).
    """
    subs = _fetch_dicts(
        conn,
        """
        SELECT id, nome, categoria_id
          FROM subcategorias_saida
         WHERE COALESCE(TRIM(nome), '') <> ''
         ORDER BY LOWER(TRIM(nome))
        """,
    )
    if subs:
        return subs
    # Fallback sem relação
    nomes = _first_nonempty_from(conn, ["subcategorias", "cadastro_subcategorias_saida"])
    return [{"id": idx + 1, "nome": n, "categoria_id": None} for idx, n in enumerate(nomes)]


def carregar_listas_para_form(db_path: str | None = None, *_, **__) -> Dict[str, Any]:
    """
    Carrega listas auxiliares para o formulário de Saída.

    Retorna dict com chaves:
      - bancos, formas, origens_dinheiro, cartoes, bandeiras
      - categorias: list[{'id','nome'}]
      - subcategorias: list[{'id','nome','categoria_id'}]
      - listar_subcategorias_por_categoria: Callable[[int|str|None], list[dict]]
    """
    def _defaults() -> Dict[str, Any]:
        return {
            "bancos": DEFAULT_BANCOS[:],
            "formas": DEFAULT_FORMAS[:],
            "origens_dinheiro": DEFAULT_ORIGENS[:],
            "categorias": [],
            "subcategorias": [],
            "cartoes": [],
            "bandeiras": DEFAULT_BANDEIRAS[:],
            # Provider default (sem DB): filtra por categoria_id se existir
            "listar_subcategorias_por_categoria": lambda categoria_id: [],
        }

    valid_path = isinstance(db_path, (str, bytes, bytearray)) and str(db_path).strip() != ""
    if not sqlite3 or not valid_path:
        return _defaults()

    conn = None
    try:
        conn = _open_sqlite(str(db_path))

        bancos = _try_get_bancos(conn)
        cartoes = _try_get_cartoes(conn)
        bandeiras = _try_get_bandeiras(conn)
        categorias = _get_categorias_full(conn)          # [{'id','nome'}]
        subcategorias = _get_subcategorias_full(conn)    # [{'id','nome','categoria_id'}]

        # Provider que respeita a relação categoria -> subcategorias (via FK)
        def _provider_subs_por_categoria(categoria_id: Any) -> List[Dict[str, Any]]:
            try:
                if categoria_id in (None, "", 0, "0"):
                    return []
                # categoria_id pode vir como str; normaliza para int quando possível
                try:
                    cat_id = int(categoria_id)
                except Exception:
                    cat_id = categoria_id
                return [s for s in subcategorias if s.get("categoria_id") == cat_id]
            except Exception:
                return []

        return {
            "bancos": bancos or DEFAULT_BANCOS[:],
            "formas": DEFAULT_FORMAS[:],
            "origens_dinheiro": DEFAULT_ORIGENS[:],
            "categorias": categorias,
            "subcategorias": subcategorias,
            "cartoes": cartoes or [],
            "bandeiras": bandeiras or DEFAULT_BANDEIRAS[:],
            "listar_subcategorias_por_categoria": _provider_subs_por_categoria,
        }
    except Exception:
        return _defaults()
    finally:
        with contextlib.suppress(Exception):
            if conn:
                conn.close()


# =============================================================================
# Shims de compatibilidade (APIs antigas usadas pela page_lancamentos)
# =============================================================================
def _spread_args_to_kwargs(args: tuple, kwargs: dict, order: List[str]) -> dict:
    """Mapeia args posicionais para kwargs seguindo 'order', sem sobrescrever chaves já existentes."""
    out = dict(kwargs)
    for i, v in enumerate(args):
        if i < len(order) and order[i] not in out:
            out[order[i]] = v
    return out


def registrar_saida(*args, **kwargs):
    """
    Alias compatível da API antiga.
    Ordem posicional aceita:
      caminho_banco, valor, forma, origem, banco, categoria, subcategoria, descricao,
      usuario, data, juros, multa, desconto, trans_uid,
      tipo_obrigacao, obrigacao_id, obrigacao_id_fatura, obrigacao_id_boleto, obrigacao_id_emprestimo
    """
    order = [
        "caminho_banco", "valor", "forma", "origem", "banco",
        "categoria", "subcategoria", "descricao", "usuario", "data",
        "juros", "multa", "desconto", "trans_uid",
        "tipo_obrigacao", "obrigacao_id",
        "obrigacao_id_fatura", "obrigacao_id_boleto", "obrigacao_id_emprestimo",
    ]
    kw = _spread_args_to_kwargs(args, kwargs, order)

    # Sinônimos
    if "origem" not in kw and "origem_dinheiro" in kw:
        kw["origem"] = kw.get("origem_dinheiro")
    if "banco" not in kw and "banco_nome" in kw:
        kw["banco"] = kw.get("banco_nome")

    return registrar_saida_action(**kw)


def registrar_saida_dinheiro(*args, **kwargs):
    """Alias compatível: força forma 'DINHEIRO' e usa 'origem_dinheiro' se fornecida."""
    order = [
        "caminho_banco", "valor", "origem_dinheiro",
        "categoria", "subcategoria", "descricao", "usuario", "data",
        "juros", "multa", "desconto", "trans_uid",
        "tipo_obrigacao", "obrigacao_id",
        "obrigacao_id_fatura", "obrigacao_id_boleto", "obrigacao_id_emprestimo",
    ]
    kw = _spread_args_to_kwargs(args, kwargs, order)
    kw.setdefault("forma", "DINHEIRO")
    if "origem" not in kw and "origem_dinheiro" in kw:
        kw["origem"] = kw.get("origem_dinheiro")
    return registrar_saida_action(**kw)


def registrar_saida_bancaria(*args, **kwargs):
    """Alias compatível para saídas bancárias (PIX/DÉBITO)."""
    order = [
        "caminho_banco", "valor", "forma", "banco_nome",
        "categoria", "subcategoria", "descricao", "usuario", "data",
        "juros", "multa", "desconto", "trans_uid",
        "tipo_obrigacao", "obrigacao_id",
        "obrigacao_id_fatura", "obrigacao_id_boleto", "obrigacao_id_emprestimo",
    ]
    kw = _spread_args_to_kwargs(args, kwargs, order)
    if "banco" not in kw and "banco_nome" in kw:
        kw["banco"] = kw.get("banco_nome")
    return registrar_saida_action(**kw)


__all__ = [
    "registrar_saida_action",
    "registrar_saida_dinheiro_action",
    "registrar_saida_bancaria_action",
    "carregar_listas_para_form",
    # aliases antigos:
    "registrar_saida",
    "registrar_saida_dinheiro",
    "registrar_saida_bancaria",
]
