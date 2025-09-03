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
from shared.ids import sanitize, uid_saida_bancaria, uid_saida_dinheiro  # noqa: E402
from services.ledger.service_ledger_infra import (  # noqa: E402
    _ensure_mov_cols,
    _fmt_obs_saida,
)

logger = logging.getLogger(__name__)

__all__ = ["_SaidasLedgerMixin"]

_EPS = 0.005  # tolerância para consideração de “sobras” não aplicadas


class _SaidasLedgerMixin:
    """Regras de saída (dinheiro e bancária). Fornece o dispatcher e os dois fluxos."""

    # ------------------------ Helpers de normalização ------------------------

    def _sane(self, v: Any) -> Optional[str]:
        """Converte para string sanitizada (ou None) antes de gerar UIDs/SQL.

        Evita erros como len(int) quando geradores de UID assumem strings.
        """
        if v is None:
            return None
        try:
            s = str(v)
        except Exception:
            return None
        return sanitize(s)

    def _parse_money(self, v: Any) -> float:
        """Converte valores monetários aceitando padrões BR e US.

        Exemplos aceitos:
            '1.234,56' → 1234.56
            '1234,56'  → 1234.56
            '1,234.56' → 1234.56
            '1234.56'  → 1234.56
            1234.56    → 1234.56
        """
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)

        s = str(v).strip()
        if not s:
            return 0.0

        # Decide qual é o separador decimal pela última ocorrência de '.' ou ','
        dot = s.rfind(".")
        comma = s.rfind(",")

        try:
            if dot == -1 and comma == -1:
                # Só dígitos
                return float(s)
            if dot > comma:
                # Provável estilo US: '.' decimal, ',' milhar
                s = s.replace(",", "")
                return float(s)
            else:
                # Provável estilo BR: ',' decimal, '.' milhar
                s = s.replace(".", "").replace(",", ".")
                return float(s)
        except Exception:
            # Fallback conservador
            try:
                return float(s.replace(",", "."))
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
        # Fallback: ISO-like
        try:
            return datetime.fromisoformat(s).date().strftime("%Y-%m-%d")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d")

    def _infer_forma(self, *, forma: Optional[str], banco: Optional[str], origem: Optional[str]) -> str:
        """Normaliza a forma para uma das: 'DINHEIRO', 'PIX', 'DÉBITO'.

        Regras:
          - usa forma/metodo/meio_pagamento já normalizados se válidos;
          - se origem for 'Caixa'/'Caixa 2' => 'DINHEIRO';
          - se houver banco => 'PIX';
          - caso contrário => 'DINHEIRO'.
        """
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
        descricao: Optional[str] = None,
        usuario: Optional[str] = None,
        trans_uid: Optional[str] = None,
        data_evento: Optional[str] = None,
        **_ignored: Any,                        # engole kwargs extras do UI/legado
    ) -> Tuple[int, int]:
        """Registrador genérico usado pelo LedgerService para **saídas avulsas**.

        Comportamento:
            - Para tipo_evento='SAIDA':
                • se forma inferida == 'DINHEIRO' → chama `registrar_saida_dinheiro(...)`
                • se forma inferida in {'PIX','DÉBITO'} → chama `registrar_saida_bancaria(...)`
            - Para 'ENTRADA': não suportado neste mixin.
        Retorno:
            (id_saida, id_mov_bancaria) ou (-1, -1) se a transação idempotente já existir.

        Raises:
            NotImplementedError: para eventos do tipo 'ENTRADA'.
            ValueError: se o valor da saída for inválido (<= 0).
        """
        tipo = (self._sane(tipo_evento) or "SAIDA").upper()
        if tipo != "SAIDA":
            raise NotImplementedError(
                "registrar_lancamento: apenas SAIDA é suportado neste mixin. "
                "Use o fluxo de entradas específico para ENTRADA."
            )

        valor = self._parse_money(valor_evento)
        if valor <= 0:
            raise ValueError("Valor da saída deve ser maior que zero.")

        # Normaliza data e campos textuais
        data = self._parse_date(data_evento)
        categoria = self._sane(categoria_evento)
        subcategoria = self._sane(subcategoria_evento)
        desc = self._sane(descricao)
        usuario_s = self._sane(usuario) or "-"

        # Escolhe forma efetiva (considera aliases)
        forma_eff = self._infer_forma(
            forma=(forma or forma_pagamento or metodo or meio_pagamento),
            banco=banco,
            origem=origem,
        )

        if forma_eff == "DINHEIRO":
            # origem_dinheiro precisa ser 'Caixa' ou 'Caixa 2'
            origem_din = origem if origem in ("Caixa", "Caixa 2") else "Caixa"
            return self.registrar_saida_dinheiro(
                data=data,
                valor=valor,
                origem_dinheiro=origem_din,
                categoria=categoria,
                sub_categoria=subcategoria,
                descricao=desc,
                usuario=usuario_s,
                trans_uid=trans_uid,  # pode ser None; será normalizado dentro
            )

        # PIX ou DÉBITO
        banco_nome = self._sane(banco) or (self._sane(origem) or "Banco 1")
        return self.registrar_saida_bancaria(
            data=data,
            valor=valor,
            banco_nome=banco_nome,
            forma=forma_eff,  # 'PIX' ou 'DÉBITO'
            categoria=categoria,
            sub_categoria=subcategoria,
            descricao=desc,
            usuario=usuario_s,
            trans_uid=trans_uid,  # pode ser None; será normalizado dentro
        )

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
        obrigacao_id_fatura: Optional[int] = None,
    ) -> Tuple[int, int]:
        """Registra uma saída **em dinheiro** (Caixa/Caixa 2) e integra com CAP (opcional).

        Regras:
            - Atualiza `saldos_caixas` na coluna validada ('caixa' ou 'caixa_2').
            - Loga a movimentação em `movimentacoes_bancarias` (origem='saidas').
            - Se `obrigacao_id_fatura` for informado, tenta pagar fatura por obrigação
              com prioridade; anota eventual sobra não aplicada no log da mov.

        Returns:
            (id_saida, id_mov_bancaria) ou (-1, -1) se trans_uid já existir.
        """
        if float(valor) <= 0:
            raise ValueError("Valor deve ser maior que zero.")
        if origem_dinheiro not in ("Caixa", "Caixa 2"):
            raise ValueError("Origem do dinheiro inválida (use 'Caixa' ou 'Caixa 2').")

        categoria = self._sane(categoria)
        sub_categoria = self._sane(sub_categoria)
        descricao = self._sane(descricao)
        usuario = self._sane(usuario) or "-"

        # **IMPORTANTE**: valor como string para o gerador de UID (evita len(int))
        valor_str = f"{float(valor):.2f}"

        tuid = trans_uid or uid_saida_dinheiro(
            data, valor_str, origem_dinheiro, categoria, sub_categoria, descricao, usuario
        )
        trans_uid_str = str(tuid)

        if getattr(self, "mov_repo", None) and hasattr(self.mov_repo, "ja_existe_transacao"):
            if self.mov_repo.ja_existe_transacao(trans_uid_str):
                logger.info("registrar_saida_dinheiro: trans_uid já existe (%s) — ignorando", trans_uid_str)
                return (-1, -1)

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()
            self._garantir_linha_saldos_caixas(conn, data)

            # (1) INSERT saida
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

            # (2) Ajusta saldos de caixa com coluna validada (whitelist)
            col_map = {"Caixa": "caixa", "Caixa 2": "caixa_2"}
            col = col_map.get(origem_dinheiro)
            cur.execute(
                f"UPDATE saldos_caixas SET {col} = COALESCE({col},0) - ? WHERE data = ?",
                (float(valor), data),
            )

            # (3) Log movimentação bancária
            _ensure_mov_cols(cur)
            obs = _fmt_obs_saida(
                forma="DINHEIRO",
                valor=float(valor),
                categoria=categoria,
                subcategoria=sub_categoria,
                descricao=descricao,
            )
            cur.execute(
                """
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao,
                     referencia_tabela, referencia_id, trans_uid, usuario, data_hora)
                VALUES (?, ?, 'saida', ?, 'saidas', ?, 'saida', ?, ?, ?, ?)
                """,
                (
                    data,
                    origem_dinheiro,
                    float(valor),
                    obs,
                    id_saida,
                    trans_uid_str,
                    usuario,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            id_mov = int(cur.lastrowid)

            # (3.1) Pagamento direto de fatura (prioritário)
            if obrigacao_id_fatura:
                sobra = self._pagar_fatura_por_obrigacao(
                    conn,
                    obrigacao_id=int(obrigacao_id_fatura),
                    valor=float(valor),
                    data_evento=data,
                    forma_pagamento="DINHEIRO",
                    origem=origem_dinheiro,
                    ledger_id=id_saida,
                    usuario=usuario,
                )
                self._registrar_sobra_obs(cur, id_mov, sobra)
                conn.commit()
                logger.debug(
                    "registrar_saida_dinheiro: id_saida=%s id_mov=%s sobra=%.2f",
                    id_saida,
                    id_mov,
                    sobra,
                )
                return (id_saida, id_mov)

            # (4) Vínculo direto com um título (opcional)
            if vinculo_pagamento:
                obrig_id = int(vinculo_pagamento["obrigacao_id"])
                tipo_obrig = str(vinculo_pagamento["tipo_obrigacao"])
                val = float(vinculo_pagamento.get("valor_pagar", vinculo_pagamento.get("valor_pago", valor)))

                self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id=obrig_id,
                    tipo_obrigacao=tipo_obrig,
                    valor_pago=val,
                    data_evento=data,
                    forma_pagamento="DINHEIRO",
                    origem=origem_dinheiro,
                    ledger_id=id_saida,
                    usuario=usuario,
                )
                self._atualizar_status_por_obrigacao(conn, obrig_id)

            # (5) Classificação + Auto-baixa por destino/tipo
            if pagamento_tipo and pagamento_destino:
                self._classificar_conta_a_pagar_por_destino(conn, pagamento_tipo, pagamento_destino)
                restante = self._auto_baixar_pagamentos(
                    conn,
                    pagamento_tipo=pagamento_tipo,
                    pagamento_destino=pagamento_destino,
                    valor_total=float(valor),
                    data_evento=data,
                    forma_pagamento="DINHEIRO",
                    origem=origem_dinheiro,
                    ledger_id=id_saida,
                    usuario=usuario,
                    competencia_pagamento=competencia_pagamento,
                )
                self._registrar_sobra_obs(cur, id_mov, restante)

            conn.commit()
            logger.debug("registrar_saida_dinheiro: id_saida=%s id_mov=%s", id_saida, id_mov)
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
        obrigacao_id_fatura: Optional[int] = None,
    ) -> Tuple[int, int]:
        """Registra uma saída **bancária** (PIX/DÉBITO) e integra com CAP (opcional).

        Regras:
            - Atualiza `saldos_bancos` pela coluna dinâmica (via `_ajustar_banco_dynamic`).
            - Loga a movimentação em `movimentacoes_bancarias` (origem='saidas').
            - Se `obrigacao_id_fatura` for informado, tenta pagar fatura por obrigação
              com prioridade; anota eventual sobra não aplicada no log da mov.

        Returns:
            (id_saida, id_mov_bancaria) ou (-1, -1) se trans_uid já existir.
        """
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

        # **IMPORTANTE**: valor como string para o gerador de UID (evita len(int))
        valor_str = f"{float(valor):.2f}"

        tuid = trans_uid or uid_saida_bancaria(
            data, valor_str, banco_nome, forma_u, categoria, sub_categoria, descricao, usuario
        )
        trans_uid_str = str(tuid)

        if getattr(self, "mov_repo", None) and hasattr(self.mov_repo, "ja_existe_transacao"):
            if self.mov_repo.ja_existe_transacao(trans_uid_str):
                logger.info("registrar_saida_bancaria: trans_uid já existe (%s) — ignorando", trans_uid_str)
                return (-1, -1)

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

            # (2) Ajusta saldos de bancos
            self._garantir_linha_saldos_bancos(conn, data)
            self._ajustar_banco_dynamic(conn, banco_col=banco_nome, delta=-float(valor), data=data)

            # (3) Log movimentação bancária
            _ensure_mov_cols(cur)
            obs = _fmt_obs_saida(
                forma=forma_u,
                valor=float(valor),
                categoria=categoria,
                subcategoria=sub_categoria,
                descricao=descricao,
                banco=(banco_nome if forma_u == "DÉBITO" else None),  # PIX não exibe nome do banco
            )
            cur.execute(
                """
                INSERT INTO movimentacoes_bancarias
                    (data, banco, tipo, valor, origem, observacao,
                     referencia_tabela, referencia_id, trans_uid, usuario, data_hora)
                VALUES (?, ?, 'saida', ?, 'saidas', ?, 'saida', ?, ?, ?, ?)
                """,
                (
                    data,
                    banco_nome,
                    float(valor),
                    obs,
                    id_saida,
                    trans_uid_str,
                    usuario,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            id_mov = int(cur.lastrowid)

            # (3.1) Pagamento direto de fatura (prioritário)
            if obrigacao_id_fatura:
                sobra = self._pagar_fatura_por_obrigacao(
                    conn,
                    obrigacao_id=int(obrigacao_id_fatura),
                    valor=float(valor),
                    data_evento=data,
                    forma_pagamento=forma_u,
                    origem=banco_nome,
                    ledger_id=id_saida,
                    usuario=usuario,
                )
                self._registrar_sobra_obs(cur, id_mov, sobra)
                conn.commit()
                logger.debug(
                    "registrar_saida_bancaria: id_saida=%s id_mov=%s sobra=%.2f",
                    id_saida,
                    id_mov,
                    sobra,
                )
                return (id_saida, id_mov)

            # (4) Vínculo direto com um título (opcional)
            if vinculo_pagamento:
                obrig_id = int(vinculo_pagamento["obrigacao_id"])
                tipo_obrig = str(vinculo_pagamento["tipo_obrigacao"])
                val = float(vinculo_pagamento.get("valor_pagar", vinculo_pagamento.get("valor_pago", valor)))

                self.cap_repo.registrar_pagamento(
                    conn,
                    obrigacao_id=obrig_id,
                    tipo_obrigacao=tipo_obrig,
                    valor_pago=val,
                    data_evento=data,
                    forma_pagamento=forma_u,
                    origem=banco_nome,
                    ledger_id=id_saida,
                    usuario=usuario,
                )
                self._atualizar_status_por_obrigacao(conn, obrig_id)

            # (5) Classificação + Auto-baixa por destino/tipo
            if pagamento_tipo and pagamento_destino:
                self._classificar_conta_a_pagar_por_destino(conn, pagamento_tipo, pagamento_destino)
                restante = self._auto_baixar_pagamentos(
                    conn,
                    pagamento_tipo=pagamento_tipo,
                    pagamento_destino=pagamento_destino,
                    valor_total=float(valor),
                    data_evento=data,
                    forma_pagamento=forma_u,
                    origem=banco_nome,
                    ledger_id=id_saida,
                    usuario=usuario,
                    competencia_pagamento=competencia_pagamento,
                )
                self._registrar_sobra_obs(cur, id_mov, restante)

            conn.commit()
            logger.debug("registrar_saida_bancaria: id_saida=%s id_mov=%s", id_saida, id_mov)
            return (id_saida, id_mov)
