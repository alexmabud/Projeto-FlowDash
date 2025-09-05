# services/ledger/service_ledger_saida.py
"""
Saídas (dinheiro e bancária).

Registra saídas em dinheiro (Caixa/Caixa 2) e bancárias (PIX/DÉBITO),
logando em `movimentacoes_bancarias` e integrando com CAP quando aplicável.

Compat e robustez:
- Aceita valores como '400,00', '1.234,56' ou '1234.56' (parser monetário BR/US-aware).
- Aceita datas em 'YYYY-MM-DD', 'YYYY/MM/DD', 'DD/MM/YYYY' ou 'DD-MM-YYYY'.
- Aceita aliases de forma: forma/forma_pagamento/metodo/meio_pagamento.
- Garante strings seguras para sanitização e idempotência (evita len(int) nos UIDs).

Integrações CAP (novas):
- Suporte a pagamento de BOLETO e EMPRESTIMO além de FATURA_CARTAO.
- Sempre usa **saida_total = principal + juros + multa** (desconto NÃO sai do caixa).
- Para FATURA_CARTAO evitamos duplicidade de mov. usando `ledger_id` no service de fatura.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

# ---------------------------------------------------------------------
# sys.path bootstrap (execução via Streamlit/CLI)
# ---------------------------------------------------------------------
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Internos
from shared.db import get_conn  # noqa: E402
from shared.ids import sanitize  # noqa: E402
from services.ledger.service_ledger_infra import (  # noqa: E402
    _fmt_obs_saida,
    log_mov_bancaria,
    gerar_trans_uid,
)
from services.ledger.service_ledger_boleto import ServiceLedgerBoleto  # noqa: E402
from services.ledger.service_ledger_fatura import ServiceLedgerFatura  # noqa: E402

logger = logging.getLogger(__name__)

__all__ = ["_SaidasLedgerMixin"]

_EPS = 0.005  # tolerância para consideração de “sobras” não aplicadas


class _SaidasLedgerMixin:
    """Regras de saída (dinheiro e bancária). Fornece o dispatcher e os dois fluxos."""

    # ------------------------ Helpers de normalização ------------------------

    def _sane(self, v: Any) -> Optional[str]:
        """Converte para string sanitizada (ou None) antes de gerar UIDs/SQL."""
        if v is None:
            return None
        try:
            s = str(v)
        except Exception:
            return None
        return sanitize(s)

    def _parse_money(self, v: Any) -> float:
        """Converte valores monetários aceitando padrões BR e US."""
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return 0.0
        dot = s.rfind("."); comma = s.rfind(",")
        try:
            if dot == -1 and comma == -1:
                return float(s)
            if dot > comma:
                s = s.replace(",", "")
                return float(s)
            else:
                s = s.replace(".", "").replace(",", ".")
                return float(s)
        except Exception:
            try:
                return float(s.replace(",", "."))  # último recurso
            except Exception:
                return 0.0

    def _parse_date(self, s: Optional[str]) -> str:
        """Normaliza para 'YYYY-MM-DD' aceitando formatos comuns."""
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

    def _infer_forma(self, *, forma: Optional[str], banco: Optional[str], origem: Optional[str]) -> str:
        """Normaliza a forma para 'DINHEIRO', 'PIX' ou 'DÉBITO'."""
        if forma:
            f = self._sane(forma)
            f = (f or "").upper()
            if f == "DEBITO":
                f = "DÉBITO"
            if f in {"DINHEIRO", "PIX", "DÉBITO"}:
                return f
        origem_norm = (origem or "").strip().lower()
        if origem_norm in {"caixa", "caixa 2", "caixa2"}:
            return "DINHEIRO"
        if banco:
            return "PIX"
        return "DINHEIRO"

    def _registrar_sobra_obs(self, cur, mov_id: int, sobra: float) -> None:
        """Anota em `movimentacoes_bancarias.observacao` a sobra não aplicada (se houver)."""
        if sobra > _EPS:
            cur.execute(
                """
                UPDATE movimentacoes_bancarias
                   SET observacao = COALESCE(observacao,'') || ' [Sobra não aplicada: R$ ' || printf('%.2f', ?) || ']'
                 WHERE id = ?
                """,
                (float(sobra), mov_id),
            )

    # -------------------------- Helpers de services --------------------------

    def _svc_boleto(self) -> ServiceLedgerBoleto:
        # Este mixin assume que self.db_path existe no objeto “pai”
        return ServiceLedgerBoleto(self.db_path)

    def _svc_fatura(self) -> ServiceLedgerFatura:
        return ServiceLedgerFatura(self.db_path)

    # ------------------------- API de compatibilidade -------------------------

    def registrar_lancamento(
        self,
        *,
        tipo_evento: str,                       # 'SAIDA' ou 'ENTRADA'
        categoria_evento: Optional[str] = None,
        subcategoria_evento: Optional[str] = None,
        valor_evento: float | str = 0.0,
        forma: Optional[str] = None,
        forma_pagamento: Optional[str] = None,  # alias compat
        metodo: Optional[str] = None,           # alias compat
        meio_pagamento: Optional[str] = None,   # alias compat
        origem: Optional[str] = None,
        banco: Optional[str] = None,
        # encargos opcionais (pass-through)
        juros: float | str = 0.0,
        multa: float | str = 0.0,
        desconto: float | str = 0.0,
        descricao: Optional[str] = None,
        usuario: Optional[str] = None,
        trans_uid: Optional[str] = None,
        data_evento: Optional[str] = None,
        # ---- NOVOS (integração CAP genérica) ----
        tipo_obrigacao: Optional[str] = None,   # 'BOLETO' | 'FATURA_CARTAO' | 'EMPRESTIMO'
        obrigacao_id: Optional[int] = None,
        obrigacao_id_fatura: Optional[int] = None,  # legado (continua aceito)
        **_ignored: Any,
    ) -> Tuple[int, int]:
        """Registrador genérico. Redireciona para saída DINHEIRO ou BANCÁRIA com integração CAP opcional."""
        tipo = (self._sane(tipo_evento) or "SAIDA").upper()
        if tipo != "SAIDA":
            raise NotImplementedError("registrar_lancamento: apenas SAIDA é suportado neste mixin.")

        valor = self._parse_money(valor_evento)
        if valor <= 0:
            raise ValueError("Valor da saída deve ser maior que zero.")

        data = self._parse_date(data_evento)
        categoria = self._sane(categoria_evento)
        subcategoria = self._sane(subcategoria_evento)
        desc = self._sane(descricao)
        usuario_s = self._sane(usuario) or "-"

        jv = self._parse_money(juros)
        mv = self._parse_money(multa)
        dv = self._parse_money(desconto)

        # Determina forma efetiva
        forma_eff = self._infer_forma(
            forma=(forma or forma_pagamento or metodo or meio_pagamento),
            banco=banco,
            origem=origem,
        )

        # Normaliza tipo/obrigação (compat mantém obrigacao_id_fatura)
        tipo_obr_norm = (self._sane(tipo_obrigacao) or (categoria or "")).upper() if (tipo_obrigacao or categoria) else None
        if obrigacao_id_fatura and not obrigacao_id:
            obrigacao_id = int(obrigacao_id_fatura)
            tipo_obr_norm = "FATURA_CARTAO"

        if forma_eff == "DINHEIRO":
            origem_din = origem if origem in ("Caixa", "Caixa 2") else "Caixa"
            return self.registrar_saida_dinheiro(
                data=data,
                valor=valor,
                origem_dinheiro=origem_din,
                categoria=categoria,
                sub_categoria=subcategoria,
                descricao=desc,
                usuario=usuario_s,
                trans_uid=trans_uid,
                juros=jv, multa=mv, desconto=dv,
                # CAP genérico (novos)
                tipo_obrigacao=tipo_obr_norm,
                obrigacao_id=obrigacao_id,
            )

        banco_nome = self._sane(banco) or (self._sane(origem) or "Banco 1")
        return self.registrar_saida_bancaria(
            data=data,
            valor=valor,
            banco_nome=banco_nome,
            forma=forma_eff,
            categoria=categoria,
            sub_categoria=subcategoria,
            descricao=desc,
            usuario=usuario_s,
            trans_uid=trans_uid,
            juros=jv, multa=mv, desconto=dv,
            # CAP genérico (novos)
            tipo_obrigacao=tipo_obr_norm,
            obrigacao_id=obrigacao_id,
        )

    def _pagar_fatura_por_obrigacao(self, conn, *args, **kwargs) -> Dict[str, Any]:
        """
        Compat: helper legado chamado por registrar_saida_* quando a categoria
        é FATURA_CARTAO. Encaminha para self.pagar_fatura_cartao(...) usando a API nova.

        Retorna dict: { sobra, saida_total, trans_uid, ... }.
        """
        if args and not kwargs:
            args = list(args) + [None] * 10
            obrigacao_id    = args[0]
            valor_base      = args[1]
            juros           = args[2] if args[2] is not None else 0.0
            multa           = args[3] if args[3] is not None else 0.0
            desconto        = args[4] if args[4] is not None else 0.0
            forma_pagamento = args[5] or "DINHEIRO"
            origem          = args[6] or "Caixa"
            data_evento     = args[7]
            observacao      = args[8]
            trans_uid       = args[9]
            kwargs = dict(
                obrigacao_id=obrigacao_id,
                valor_base=valor_base,
                juros=juros,
                multa=multa,
                desconto=desconto,
                forma_pagamento=forma_pagamento,
                origem=origem,
                data_evento=data_evento,
                observacao=observacao,
                trans_uid=trans_uid,
            )
        else:
            if "valor" in kwargs and "valor_base" not in kwargs:
                kwargs["valor_base"] = kwargs.pop("valor")
            kwargs.setdefault("juros", 0.0)
            kwargs.setdefault("multa", 0.0)
            kwargs.setdefault("desconto", 0.0)
            kwargs.setdefault("forma_pagamento", "DINHEIRO")
            kwargs.setdefault("origem", "Caixa")

        try:
            res = self.pagar_fatura_cartao(conn, **kwargs)
        except TypeError:
            res = self.pagar_fatura_cartao(**kwargs)

        # Garante chaves esperadas
        if not isinstance(res, dict):
            return {"sobra": 0.0, "saida_total": float(kwargs.get("valor_base") or 0.0), "trans_uid": ""}
        res.setdefault("sobra", 0.0)
        res.setdefault("saida_total", float(kwargs.get("valor_base") or 0.0))
        res.setdefault("trans_uid", "")
        return res

    # -------------------------- Fluxos específicos --------------------------

    def registrar_saida_dinheiro(
        self,
        *,
        data: str,
        valor: float,
        origem_dinheiro: str,
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        trans_uid: Optional[str] = None,
        vinculo_pagamento: Optional[Dict[str, Any]] = None,
        pagamento_tipo: Optional[str] = None,
        pagamento_destino: Optional[str] = None,
        competencia_pagamento: Optional[str] = None,
        obrigacao_id_fatura: Optional[int] = None,  # legado
        juros: float | str = 0.0,
        multa: float | str = 0.0,
        desconto: float | str = 0.0,
        # ---- NOVOS (CAP genérico) ----
        tipo_obrigacao: Optional[str] = None,   # 'BOLETO' | 'FATURA_CARTAO' | 'EMPRESTIMO'
        obrigacao_id: Optional[int] = None,
    ) -> Tuple[int, int]:
        """Registra uma saída **em dinheiro** (Caixa/Caixa 2) e integra com CAP (opcional)."""
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if origem_dinheiro not in ("Caixa", "Caixa 2"):
            raise ValueError("Origem do dinheiro inválida (use 'Caixa' ou 'Caixa 2').")

        categoria = self._sane(categoria)
        sub_categoria = self._sane(sub_categoria)
        descricao = self._sane(descricao)
        usuario = self._sane(usuario) or "-"

        jv = self._parse_money(juros)
        mv = self._parse_money(multa)
        dv = self._parse_money(desconto)

        # trans_uid idempotente baseado nos campos (determinístico quando não fornecido)
        valor_str = f"{float(valor):.2f}"
        seed = f"SAIDA_DIN|{data}|{valor_str}|{origem_dinheiro}|{categoria}|{sub_categoria}|{descricao}|{usuario}"
        trans_uid_str = str(trans_uid or gerar_trans_uid("mb", seed=seed))

        if getattr(self, "mov_repo", None) and hasattr(self.mov_repo, "ja_existe_transacao"):
            if self.mov_repo.ja_existe_transacao(trans_uid_str):
                logger.info("registrar_saida_dinheiro: trans_uid já existe (%s) — ignorando", trans_uid_str)
                return (-1, -1)

        # Normaliza tipo/obrigação (mantém compat com obrigacao_id_fatura)
        tipo_eff = (self._sane(tipo_obrigacao) or (categoria or "")).upper() if (tipo_obrigacao or categoria) else None
        if obrigacao_id_fatura and not obrigacao_id:
            obrigacao_id = int(obrigacao_id_fatura)
            tipo_eff = "FATURA_CARTAO"

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()
            self._garantir_linha_saldos_caixas(conn, data)

            # (1) INSERT na tabela 'saida' com o VALOR digitado (principal informado)
            cur.execute(
                """
                INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                   Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                   Origem_Dinheiro, Banco_Saida)
                VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
                """,
                (data, categoria, sub_categoria, descricao, float(valor), usuario, origem_dinheiro),
            )
            id_saida = int(cur.lastrowid)

            saida_total = float(valor)  # padrão para saídas avulsas
            sobra_reg = 0.0
            mov_desc = descricao

            # (2) Integra CAP quando informado tipo/obrigação
            if tipo_eff and obrigacao_id:
                if tipo_eff == "FATURA_CARTAO":
                    res = self._pagar_fatura_por_obrigacao(
                        conn,
                        obrigacao_id=int(obrigacao_id),
                        valor=float(valor),
                        juros=jv,
                        multa=mv,
                        desconto=dv,
                        data_evento=data,
                        forma_pagamento="DINHEIRO",
                        origem=origem_dinheiro,
                        ledger_id=id_saida,   # evita duplicidade de movimentação no service de fatura
                        usuario=usuario,
                    )
                    saida_total = float(res.get("saida_total", valor))
                    sobra_reg = float(res.get("sobra", 0.0))
                    mov_desc = descricao or f"Pagamento fatura (obrigação {obrigacao_id})"
                    mov_cat = categoria or "FATURA_CARTAO"
                elif tipo_eff in ("BOLETO", "EMPRESTIMO"):
                    res = self._svc_boleto().pagar_boleto(
                        obrigacao_id=int(obrigacao_id),
                        principal=float(valor),
                        juros=jv,
                        multa=mv,
                        desconto=dv,
                        data_evento=data,
                        usuario=usuario,
                        ledger_id=id_saida,  # evita duplicidade de movimentação no service de boleto
                        conn=conn,
                    )
                    saida_total = float(res.get("saida_total", valor))
                    # Sobra = principal digitado - principal realmente aplicado (somatório)
                    try:
                        soma_principal_aplicado = sum(float(r.get("aplicado_principal", 0.0)) for r in res.get("resultados", []))
                    except Exception:
                        soma_principal_aplicado = 0.0
                    sobra_reg = max(0.0, float(valor) - soma_principal_aplicado)
                    mov_desc = descricao or (f"Pagamento {tipo_eff.lower()} (obrigação {obrigacao_id})")
                    mov_cat = categoria or tipo_eff
                else:
                    mov_cat = categoria  # tipo não reconhecido, trata como avulsa
            else:
                mov_cat = categoria  # saída avulsa

            # (3) Ajusta saldos do CAIXA pelo **valor líquido que saiu** (saida_total)
            col_map = {"Caixa": "caixa", "Caixa 2": "caixa_2"}
            col = col_map.get(origem_dinheiro)
            cur.execute(
                f"UPDATE saldos_caixas SET {col} = COALESCE({col},0) - ? WHERE data = ?",
                (saida_total, data),
            )

            # (4) Log movimentação bancária central (idempotente por trans_uid)
            obs = _fmt_obs_saida(
                forma="DINHEIRO",
                valor=saida_total,
                categoria=mov_cat,
                subcategoria=sub_categoria,
                descricao=mov_desc,
            )
            id_mov = log_mov_bancaria(
                conn,
                data=data,
                banco=origem_dinheiro,
                tipo="saida",
                valor=saida_total,
                origem="saidas",
                observacao=obs,
                usuario=usuario,
                referencia_id=id_saida,
                referencia_tabela="saida",
                trans_uid=trans_uid_str,
                data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

            # (5) Anota sobra de principal (se houver)
            self._registrar_sobra_obs(cur, id_mov, sobra_reg)

            conn.commit()
            logger.debug(
                "registrar_saida_dinheiro: id_saida=%s id_mov=%s liquido=%.2f sobra=%.2f tipo=%s obrigacao=%s",
                id_saida, id_mov, saida_total, sobra_reg, (tipo_eff or "-"), (obrigacao_id or "-"),
            )
            return (id_saida, id_mov)

    def registrar_saida_bancaria(
        self,
        *,
        data: str,
        valor: float,
        banco_nome: str,
        forma: str,  # "PIX" ou "DÉBITO"
        categoria: Optional[str],
        sub_categoria: Optional[str],
        descricao: Optional[str],
        usuario: str,
        trans_uid: Optional[str] = None,
        vinculo_pagamento: Optional[Dict[str, Any]] = None,
        pagamento_tipo: Optional[str] = None,
        pagamento_destino: Optional[str] = None,
        competencia_pagamento: Optional[str] = None,
        obrigacao_id_fatura: Optional[int] = None,  # legado
        juros: float | str = 0.0,
        multa: float | str = 0.0,
        desconto: float | str = 0.0,
        # ---- NOVOS (CAP genérico) ----
        tipo_obrigacao: Optional[str] = None,   # 'BOLETO' | 'FATURA_CARTAO' | 'EMPRESTIMO'
        obrigacao_id: Optional[int] = None,
    ) -> Tuple[int, int]:
        """Registra uma saída **bancária** (PIX/DÉBITO) e integra com CAP (opcional)."""
        forma_u = (self._sane(forma) or "").upper()
        if forma_u == "DEBITO":
            forma_u = "DÉBITO"
        if forma_u not in ("PIX", "DÉBITO"):
            raise ValueError("Forma inválida para saída bancária.")
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")

        banco_nome = self._sane(banco_nome) or "Banco 1"
        categoria = self._sane(categoria)
        sub_categoria = self._sane(sub_categoria)
        descricao = self._sane(descricao)
        usuario = self._sane(usuario) or "-"

        jv = self._parse_money(juros)
        mv = self._parse_money(multa)
        dv = self._parse_money(desconto)

        valor_str = f"{float(valor):.2f}"
        seed = f"SAIDA_BAN|{data}|{valor_str}|{banco_nome}|{forma_u}|{categoria}|{sub_categoria}|{descricao}|{usuario}"
        trans_uid_str = str(trans_uid or gerar_trans_uid("mb", seed=seed))

        if getattr(self, "mov_repo", None) and hasattr(self.mov_repo, "ja_existe_transacao"):
            if self.mov_repo.ja_existe_transacao(trans_uid_str):
                logger.info("registrar_saida_bancaria: trans_uid já existe (%s) — ignorando", trans_uid_str)
                return (-1, -1)

        # Normaliza tipo/obrigação (mantém compat com obrigacao_id_fatura)
        tipo_eff = (self._sane(tipo_obrigacao) or (categoria or "")).upper() if (tipo_obrigacao or categoria) else None
        if obrigacao_id_fatura and not obrigacao_id:
            obrigacao_id = int(obrigacao_id_fatura)
            tipo_eff = "FATURA_CARTAO"

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()

            # (1) INSERT saida
            cur.execute(
                """
                INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                   Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                   Origem_Dinheiro, Banco_Saida)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
                """,
                (data, categoria, sub_categoria, descricao, forma_u, float(valor), usuario, banco_nome),
            )
            id_saida = int(cur.lastrowid)

            saida_total = float(valor)  # padrão para avulsas
            sobra_reg = 0.0
            mov_desc = descricao

            if tipo_eff and obrigacao_id:
                if tipo_eff == "FATURA_CARTAO":
                    res = self._pagar_fatura_por_obrigacao(
                        conn,
                        obrigacao_id=int(obrigacao_id),
                        valor=float(valor),
                        juros=jv,
                        multa=mv,
                        desconto=dv,
                        data_evento=data,
                        forma_pagamento=forma_u,
                        origem=banco_nome,
                        ledger_id=id_saida,  # evita duplicidade no service de fatura
                        usuario=usuario,
                    )
                    saida_total = float(res.get("saida_total", valor))
                    sobra_reg = float(res.get("sobra", 0.0))
                    mov_desc = descricao or f"Pagamento fatura (obrigação {obrigacao_id})"
                    mov_cat = categoria or "FATURA_CARTAO"
                elif tipo_eff in ("BOLETO", "EMPRESTIMO"):
                    res = self._svc_boleto().pagar_boleto(
                        obrigacao_id=int(obrigacao_id),
                        principal=float(valor),
                        juros=jv,
                        multa=mv,
                        desconto=dv,
                        data_evento=data,
                        usuario=usuario,
                        ledger_id=id_saida,  # evita duplicidade de movimentação no service de boleto
                        conn=conn,
                    )
                    saida_total = float(res.get("saida_total", valor))
                    try:
                        soma_principal_aplicado = sum(float(r.get("aplicado_principal", 0.0)) for r in res.get("resultados", []))
                    except Exception:
                        soma_principal_aplicado = 0.0
                    sobra_reg = max(0.0, float(valor) - soma_principal_aplicado)
                    mov_desc = descricao or (f"Pagamento {tipo_eff.lower()} (obrigação {obrigacao_id})")
                    mov_cat = categoria or tipo_eff
                else:
                    mov_cat = categoria
            else:
                mov_cat = categoria

            # (2) Ajusta saldo do banco pelo **valor líquido** (saida_total)
            self._garantir_linha_saldos_bancos(conn, data)
            self._ajustar_banco_dynamic(conn, banco_col=banco_nome, delta=-saida_total, data=data)

            # (3) Log movimentação bancária central (idempotente por trans_uid)
            obs = _fmt_obs_saida(
                forma=forma_u,
                valor=saida_total,
                categoria=mov_cat,
                subcategoria=sub_categoria,
                descricao=mov_desc,
                banco=(banco_nome if forma_u == "DÉBITO" else None),
            )
            id_mov = log_mov_bancaria(
                conn,
                data=data,
                banco=banco_nome,
                tipo="saida",
                valor=saida_total,
                origem="saidas",
                observacao=obs,
                usuario=usuario,
                referencia_id=id_saida,
                referencia_tabela="saida",
                trans_uid=trans_uid_str,
                data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

            # (4) Sobra (se houver)
            self._registrar_sobra_obs(cur, id_mov, sobra_reg)

            conn.commit()
            logger.debug(
                "registrar_saida_bancaria: id_saida=%s id_mov=%s liquido=%.2f sobra=%.2f tipo=%s obrigacao=%s",
                id_saida, id_mov, saida_total, sobra_reg, (tipo_eff or "-"), (obrigacao_id or "-"),
            )
            return (id_saida, id_mov)
