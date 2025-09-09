# flowdash_pages/lancamentos/saida/actions_saida.py
"""
Actions de Saída (dinheiro, bancária e crédito).

Responsabilidades:
- Normalizar entradas e delegar ao `LedgerService`.
- Integrar obrigações (FATURA_CARTAO, BOLETO, EMPRESTIMO) nos fluxos de pagamento.
- Manter compatibilidade com chamadas antigas (aliases de parâmetros).
- Para CRÉDITO: não cria linha em `saida`; cria CAP (contas_a_pagar_mov),
  fatura_cartao_itens e log em movimentacoes_bancarias (tipo 'registro').

Retorno padrão:
    dict: { ok: bool, id_saida: int|None, id_mov: int|None, mensagem: str|None }
"""

from __future__ import annotations

import contextlib
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import sqlite3
except Exception:  # pragma: no cover
    sqlite3 = None  # permite rodar sem sqlite em ambientes de teste

from services.ledger.service_ledger import LedgerService


# =============================================================================
# Constantes (listas auxiliares para a UI)
# =============================================================================
DEFAULT_FORMAS = ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "BOLETO"]
DEFAULT_ORIGENS = ["Caixa", "Caixa 2"]
DEFAULT_BANDEIRAS = ["VISA", "MASTERCARD", "ELO", "HIPERCARD", "AMEX"]
DEFAULT_BANCOS = ["Banco 1", "Banco 2", "Banco 3", "Banco 4"]


# =============================================================================
# Helpers de normalização
# =============================================================================
def _norm_str(v: Any) -> Optional[str]:
    """Converte para str aparada; retorna None se vazio."""
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _str_or_empty(v: Any) -> str:
    """String aparada; retorna '' se vazio/None."""
    s = _norm_str(v)
    return s if s is not None else ""


def _money(v: Any) -> float:
    """Converte valores BR/US em float. Usa o último separador como decimal."""
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
            return float(s.replace(",", ""))  # 1,234.56 -> 1234.56
        return float(s.replace(".", "").replace(",", "."))  # 1.234,56 -> 1234.56
    except Exception:
        with contextlib.suppress(Exception):
            return float(s.replace(",", "."))
        return 0.0


def _norm_date(s: Optional[str]) -> str:
    """Normaliza para 'YYYY-MM-DD'. Se inválido/None, usa hoje."""
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
    """Normaliza a forma de pagamento para o padrão da aplicação."""
    f = (forma or "").strip().upper()
    if f == "DEBITO":
        f = "DÉBITO"
    if f.startswith("CREDITO") or f.startswith("CRÉDITO"):
        f = "CRÉDITO"
    return f or "DINHEIRO"


# extrai “3x” de textos como “Crédito 3x”, “credito 10X”, etc.
_RX_PARC = re.compile(r"(\d+)\s*[Xx]\b")

def _extract_parcelas_from_forma(txt: Optional[str]) -> Optional[int]:
    t = _norm_str(txt) or ""
    m = _RX_PARC.search(t)
    if not m:
        return None
    try:
        n = int(m.group(1))
        return max(1, min(24, n))  # sanidade
    except Exception:
        return None


def _resolve_obrigacao(
    *,
    tipo_obrigacao: Optional[str],
    obrigacao_id: Optional[int],
    obrigacao_id_fatura: Optional[int],
    obrigacao_id_boleto: Optional[int],
    obrigacao_id_emprestimo: Optional[int],
) -> Tuple[Optional[str], Optional[int]]:
    """Resolve (tipo_obrigacao, obrigacao_id) aceitando campos legados."""
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
# Persistência auxiliar (fix-up em tabela `saida`)
# =============================================================================
def _open_sqlite(db_path: str):
    """Abre conexão SQLite com pragmas seguros."""
    conn = sqlite3.connect(db_path, timeout=30)
    with contextlib.suppress(Exception):
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _fixup_saida_row(
    db_path: str,
    saida_id: Optional[int],
    categoria: str,
    sub_categoria: str,
    descricao: str,
) -> None:
    """Garante `Categoria`, `Sub_Categoria` e `Descricao` na linha recém-criada."""
    if not sqlite3 or not saida_id:
        return
    try:
        conn = _open_sqlite(db_path)
        conn.execute(
            """
            UPDATE saida
               SET Categoria     = COALESCE(?, Categoria),
                   Sub_Categoria = COALESCE(?, Sub_Categoria),
                   Descricao     = COALESCE(?, Descricao)
             WHERE id = ?
            """,
            (categoria, sub_categoria, descricao, int(saida_id)),
        )
        conn.commit()
    except Exception:
        pass
    finally:
        with contextlib.suppress(Exception):
            conn.close()


# =============================================================================
# Ações principais
# =============================================================================
def registrar_saida_action(
    *,
    caminho_banco: str,
    valor: Any,
    forma: Optional[str] = None,                 # "DINHEIRO" | "PIX" | "DÉBITO" | "CRÉDITO" | "Crédito 3x"
    origem: Optional[str] = None,                # "Caixa"/"Caixa 2" ou nome da conta/cartão
    banco: Optional[str] = None,                 # conta/banco (ou cartão em UIs antigas)

    # Aliases aceitos (compat com UI/rotas antigas)
    categoria: Optional[str] = None,
    cat_nome: Optional[str] = None,
    subcategoria: Optional[str] = None,
    sub_categoria: Optional[str] = None,
    descricao: Optional[str] = None,
    descricao_final: Optional[str] = None,

    usuario: Optional[str] = None,
    data: Optional[str] = None,
    juros: Any = 0.0,
    multa: Any = 0.0,
    desconto: Any = 0.0,
    trans_uid: Optional[str] = None,

    # CRÉDITO
    parcelas: Any = None,
    cartao_nome: Optional[str] = None,
    fechamento: Optional[int] = None,
    vencimento: Optional[int] = None,

    # CAP
    tipo_obrigacao: Optional[str] = None,
    obrigacao_id: Optional[int] = None,
    obrigacao_id_fatura: Optional[int] = None,
    obrigacao_id_boleto: Optional[int] = None,
    obrigacao_id_emprestimo: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Registra uma saída. Decide o fluxo:
      - DINHEIRO  -> LedgerService.registrar_saida_dinheiro
      - PIX/DÉBITO-> LedgerService.registrar_saida_bancaria
      - CRÉDITO   -> LedgerService.registrar_lancamento (desvio p/ registrar_saida_credito)
    """
    valor_f = _money(valor)
    if valor_f <= 0:
        return {"ok": False, "mensagem": "Valor da saída deve ser maior que zero."}

    data_norm = _norm_date(data)
    forma_norm = _norm_forma(forma)

    # Resolver obrigação (mantém compat)
    tipo_obr, obr_id = _resolve_obrigacao(
        tipo_obrigacao=tipo_obrigacao,
        obrigacao_id=obrigacao_id,
        obrigacao_id_fatura=obrigacao_id_fatura,
        obrigacao_id_boleto=obrigacao_id_boleto,
        obrigacao_id_emprestimo=obrigacao_id_emprestimo,
    )

    # Normalização de campos textuais (somente o digitado)
    cat_str = _str_or_empty(categoria if _norm_str(categoria) else cat_nome)
    subcat_str = _str_or_empty(subcategoria if _norm_str(subcategoria) else sub_categoria)
    desc_user = _str_or_empty(descricao if _norm_str(descricao) else descricao_final)

    try:
        ledger = LedgerService(caminho_banco)
        usuario_s = _norm_str(usuario) or "-"

        # ---------------------- Fluxo CRÉDITO (compra) ----------------------
        if forma_norm == "CRÉDITO":
            # Se vier “Crédito 3x” em forma e parcelas não informadas, extrair do texto
            n_parc = int(parcelas or _extract_parcelas_from_forma(forma) or 1)
            n_parc = max(1, min(12, n_parc))  # sanidade simples
            cartao_eff = _norm_str(cartao_nome) or _norm_str(banco) or _norm_str(origem) or "Cartão"

            id_like, id_mov = ledger.registrar_lancamento(
                tipo_evento="SAIDA",
                categoria_evento=cat_str,
                subcategoria_evento=subcat_str,
                valor_evento=valor_f,
                forma="CRÉDITO",
                origem=_norm_str(origem),
                banco=_norm_str(banco),
                juros=_money(juros),
                multa=_money(multa),
                desconto=_money(desconto),
                descricao=desc_user,
                usuario=usuario_s,
                trans_uid=_norm_str(trans_uid),
                data_evento=data_norm,
                # extras consumidos pelo desvio de CRÉDITO no ledger:
                parcelas=n_parc,
                cartao_nome=cartao_eff,
                fechamento=int(fechamento or 0),
                vencimento=int(vencimento or 0),
            )
            # Não há linha em `saida` para fixar; devolvemos ids do fluxo
            return {"ok": True, "id_saida": id_like, "id_mov": id_mov, "mensagem": None}

        # ---------------- DINHEIRO / PIX / DÉBITO (pagamento “real”) --------
        if forma_norm == "DINHEIRO":
            id_saida, id_mov = ledger.registrar_saida_dinheiro(
                data=data_norm,
                valor=float(valor_f),
                origem_dinheiro=_norm_str(origem) or "Caixa",
                categoria=cat_str,
                sub_categoria=subcat_str,
                descricao=desc_user,
                usuario=usuario_s,
                juros=_money(juros),
                multa=_money(multa),
                desconto=_money(desconto),
                trans_uid=_norm_str(trans_uid),
                obrigacao_id_fatura=obr_id if tipo_obr == "FATURA_CARTAO" else None,
                obrigacao_id_boleto=obr_id if tipo_obr == "BOLETO" else None,
                obrigacao_id_emprestimo=obr_id if tipo_obr == "EMPRESTIMO" else None,
            )
            _fixup_saida_row(caminho_banco, id_saida, cat_str, subcat_str, desc_user)
            return {"ok": True, "id_saida": id_saida, "id_mov": id_mov, "mensagem": None}

        # Fluxo bancário (PIX/DÉBITO)
        id_saida, id_mov = ledger.registrar_saida_bancaria(
            data=data_norm,
            valor=float(valor_f),
            banco_nome=_norm_str(banco) or _norm_str(origem) or "Banco 1",
            forma=forma_norm,
            categoria=cat_str,
            sub_categoria=subcat_str,
            descricao=desc_user,
            usuario=usuario_s,
            juros=_money(juros),
            multa=_money(multa),
            desconto=_money(desconto),
            trans_uid=_norm_str(trans_uid),
            obrigacao_id_fatura=obr_id if tipo_obr == "FATURA_CARTAO" else None,
            obrigacao_id_boleto=obr_id if tipo_obr == "BOLETO" else None,
            obrigacao_id_emprestimo=obr_id if tipo_obr == "EMPRESTIMO" else None,
        )
        _fixup_saida_row(caminho_banco, id_saida, cat_str, subcat_str, desc_user)
        return {"ok": True, "id_saida": id_saida, "id_mov": id_mov, "mensagem": None}

    except Exception as e:
        return {"ok": False, "mensagem": f"Falha ao registrar saída: {e}"}


def registrar_saida_dinheiro_action(
    *,
    caminho_banco: str,
    valor: Any,
    origem_dinheiro: Optional[str] = None,
    categoria: Optional[str] = None,
    cat_nome: Optional[str] = None,
    subcategoria: Optional[str] = None,
    sub_categoria: Optional[str] = None,
    descricao: Optional[str] = None,
    descricao_final: Optional[str] = None,
    usuario: Optional[str] = None,
    data: Optional[str] = None,
    juros: Any = 0.0,
    multa: Any = 0.0,
    desconto: Any = 0.0,
    trans_uid: Optional[str] = None,
    tipo_obrigacao: Optional[str] = None,
    obrigacao_id: Optional[int] = None,
    obrigacao_id_fatura: Optional[int] = None,
    obrigacao_id_boleto: Optional[int] = None,
    obrigacao_id_emprestimo: Optional[int] = None,
) -> Dict[str, Any]:
    """Convenience: força fluxo DINHEIRO."""
    return registrar_saida_action(
        caminho_banco=caminho_banco,
        valor=valor,
        forma="DINHEIRO",
        origem=origem_dinheiro or "Caixa",
        categoria=categoria,
        cat_nome=cat_nome,
        subcategoria=subcategoria,
        sub_categoria=sub_categoria,
        descricao=descricao,
        descricao_final=descricao_final,
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
    forma: str,                                  # "PIX" | "DÉBITO" | "CRÉDITO"
    banco_nome: Optional[str] = None,
    categoria: Optional[str] = None,
    cat_nome: Optional[str] = None,
    subcategoria: Optional[str] = None,
    sub_categoria: Optional[str] = None,
    descricao: Optional[str] = None,
    descricao_final: Optional[str] = None,
    usuario: Optional[str] = None,
    data: Optional[str] = None,
    juros: Any = 0.0,
    multa: Any = 0.0,
    desconto: Any = 0.0,
    trans_uid: Optional[str] = None,
    # Se chamar com CRÉDITO por aqui, também funciona:
    parcelas: Any = None,
    cartao_nome: Optional[str] = None,
    fechamento: Optional[int] = None,
    vencimento: Optional[int] = None,
    # CAP
    tipo_obrigacao: Optional[str] = None,
    obrigacao_id: Optional[int] = None,
    obrigacao_id_fatura: Optional[int] = None,
    obrigacao_id_boleto: Optional[int] = None,
    obrigacao_id_emprestimo: Optional[int] = None,
) -> Dict[str, Any]:
    """Convenience: fluxo bancário (PIX/DÉBITO) e também CRÉDITO (encaminha p/ registrar_lancamento)."""
    if _norm_forma(forma) == "CRÉDITO":
        return registrar_saida_action(
            caminho_banco=caminho_banco,
            valor=valor,
            forma="CRÉDITO",
            banco=banco_nome,
            categoria=categoria,
            cat_nome=cat_nome,
            subcategoria=subcategoria,
            sub_categoria=sub_categoria,
            descricao=descricao,
            descricao_final=descricao_final,
            usuario=usuario,
            data=data,
            juros=juros,
            multa=multa,
            desconto=desconto,
            trans_uid=trans_uid,
            parcelas=parcelas,
            cartao_nome=cartao_nome or banco_nome,
            fechamento=fechamento,
            vencimento=vencimento,
        )
    # PIX/DÉBITO padrão:
    return registrar_saida_action(
        caminho_banco=caminho_banco,
        valor=valor,
        forma=forma,
        banco=banco_nome,
        categoria=categoria,
        cat_nome=cat_nome,
        subcategoria=subcategoria,
        sub_categoria=sub_categoria,
        descricao=descricao,
        descricao_final=descricao_final,
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
# Utilitários de listas para o formulário
# =============================================================================
def _fetch_names(conn, sql: str) -> List[str]:
    """Executa SELECT de única coluna e retorna lista de strings limpas."""
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
    """Executa SELECT e retorna lista de dicts (coluna->valor)."""
    with contextlib.suppress(Exception):
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        out = [{k: r[k] for k in r.keys()} for r in rows]
        conn.row_factory = None
        return out
    return []


def _first_nonempty_from(conn, tables: Iterable[str], column: str = "nome") -> List[str]:
    """Retorna a primeira lista não-vazia de nomes dentre tabelas candidatas."""
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
    lst = _first_nonempty_from(conn, ["cadastro_bancos", "bancos"])
    if lst:
        return lst
    with contextlib.suppress(Exception):
        cur = conn.execute("PRAGMA table_info('saldos_bancos')")
        colunas = [c[1] for c in cur.fetchall()]
        ignorar = {"id", "data", "created_at", "updated_at"}
        candidatos = [c for c in colunas if c not in ignorar]
        if candidatos:
            return sorted(candidatos, key=lambda s: s.lower().strip())
    return DEFAULT_BANCOS[:]


def _try_get_cartoes(conn) -> List[str]:
    return _first_nonempty_from(conn, ["cartoes_credito", "cartoes"])


def _try_get_bandeiras(conn) -> List[str]:
    return _first_nonempty_from(conn, ["bandeiras_cartao", "cartoes_bandeiras", "bandeiras"]) or DEFAULT_BANDEIRAS[:]


def _get_categorias_full(conn) -> List[Dict[str, Any]]:
    """Retorna categorias com IDs reais; fallback para nomes simples."""
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
    nomes = _first_nonempty_from(conn, ["categorias", "cadastro_categorias_saida"])
    return [{"id": idx + 1, "nome": n} for idx, n in enumerate(nomes)]


def _get_subcategorias_full(conn) -> List[Dict[str, Any]]:
    """Retorna subcategorias com FK de categoria; fallback plano."""
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
    nomes = _first_nonempty_from(conn, ["subcategorias", "cadastro_subcategorias_saida"])
    return [{"id": idx + 1, "nome": n, "categoria_id": None} for idx, n in enumerate(nomes)]


def carregar_listas_para_form(db_path: str | None = None, *_: Any, **__: Any) -> Dict[str, Any]:
    """
    Carrega listas auxiliares para o formulário de Saída.

    Retorna dict:
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
            "listar_subcategorias_por_categoria": lambda _categoria_id=None: [],
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
        categorias = _get_categorias_full(conn)
        subcategorias = _get_subcategorias_full(conn)

        def _provider_subs_por_categoria(categoria_id: Any) -> List[Dict[str, Any]]:
            try:
                if categoria_id in (None, "", 0, "0"):
                    return []
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
# Compatibilidade (APIs antigas)
# =============================================================================
def _spread_args_to_kwargs(args: tuple, kwargs: dict, order: List[str]) -> dict:
    """Mapeia args posicionais para kwargs seguindo `order`, sem sobrescrever já passados."""
    out = dict(kwargs)
    for i, v in enumerate(args):
        if i < len(order) and order[i] not in out:
            out[order[i]] = v
    return out


def registrar_saida(*args, **kwargs):
    """Compat: aceita ordem posicional antiga e aliases de chaves."""
    order = [
        "caminho_banco", "valor", "forma", "origem", "banco",
        "categoria", "subcategoria", "descricao", "usuario", "data",
        "juros", "multa", "desconto", "trans_uid",
        # crédito (se vierem):
        "parcelas", "cartao_nome", "fechamento", "vencimento",
        # CAP:
        "tipo_obrigacao", "obrigacao_id",
        "obrigacao_id_fatura", "obrigacao_id_boleto", "obrigacao_id_emprestimo",
    ]
    kw = _spread_args_to_kwargs(args, kwargs, order)

    # Aliases vindos do form/page
    if "categoria" not in kw and "cat_nome" in kw:
        kw["categoria"] = kw.get("cat_nome")
    if "subcategoria" not in kw and "sub_categoria" in kw:
        kw["subcategoria"] = kw.get("sub_categoria")
    if "descricao" not in kw and "descricao_final" in kw:
        kw["descricao"] = kw.get("descricao_final")

    # Aliases de origem/banco
    if "origem" not in kw and "origem_dinheiro" in kw:
        kw["origem"] = kw.get("origem_dinheiro")
    if "banco" not in kw and "banco_nome" in kw:
        kw["banco"] = kw.get("banco_nome")

    return registrar_saida_action(**kw)


def registrar_saida_dinheiro(*args, **kwargs):
    """Compat: força forma 'DINHEIRO' e usa 'origem_dinheiro' se fornecida."""
    order = [
        "caminho_banco", "valor", "origem_dinheiro",
        "categoria", "subcategoria", "descricao", "usuario", "data",
        "juros", "multa", "desconto", "trans_uid",
        "tipo_obrigacao", "obrigacao_id",
        "obrigacao_id_fatura", "obrigacao_id_boleto", "obrigacao_id_emprestimo",
    ]
    kw = _spread_args_to_kwargs(args, kwargs, order)

    if "categoria" not in kw and "cat_nome" in kw:
        kw["categoria"] = kw.get("cat_nome")
    if "subcategoria" not in kw and "sub_categoria" in kw:
        kw["subcategoria"] = kw.get("sub_categoria")
    if "descricao" not in kw and "descricao_final" in kw:
        kw["descricao"] = kw.get("descricao_final")

    kw.setdefault("forma", "DINHEIRO")
    if "origem" not in kw and "origem_dinheiro" in kw:
        kw["origem"] = kw.get("origem_dinheiro")
    return registrar_saida_action(**kw)


def registrar_saida_bancaria(*args, **kwargs):
    """Compat: fluxo bancário (PIX/DÉBITO) e CRÉDITO (encaminha para registrar_lancamento)."""
    order = [
        "caminho_banco", "valor", "forma", "banco_nome",
        "categoria", "subcategoria", "descricao", "usuario", "data",
        "juros", "multa", "desconto", "trans_uid",
        # crédito (se vierem):
        "parcelas", "cartao_nome", "fechamento", "vencimento",
        # CAP:
        "tipo_obrigacao", "obrigacao_id",
        "obrigacao_id_fatura", "obrigacao_id_boleto", "obrigacao_id_emprestimo",
    ]
    kw = _spread_args_to_kwargs(args, kwargs, order)

    if "categoria" not in kw and "cat_nome" in kw:
        kw["categoria"] = kw.get("cat_nome")
    if "subcategoria" not in kw and "sub_categoria" in kw:
        kw["subcategoria"] = kw.get("sub_categoria")
    if "descricao" not in kw and "descricao_final" in kw:
        kw["descricao"] = kw.get("descricao_final")

    if "banco" not in kw and "banco_nome" in kw:
        kw["banco"] = kw.get("banco_nome")
    return registrar_saida_action(**kw)


__all__ = [
    "registrar_saida_action",
    "registrar_saida_dinheiro_action",
    "registrar_saida_bancaria_action",
    "carregar_listas_para_form",
    # compat:
    "registrar_saida",
    "registrar_saida_dinheiro",
    "registrar_saida_bancaria",
]
