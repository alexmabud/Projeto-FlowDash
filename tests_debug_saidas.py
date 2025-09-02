# tests_debug_saidas.py
from __future__ import annotations

import os
import sys
import traceback
from datetime import datetime

# --- Ajuste o caminho se seu projeto estiver em outra pasta ---
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

DB_PATH = os.path.join(PROJECT_ROOT, "data", "flowdash_data.db")

def run_case(title, func):
    print("\n" + "="*80)
    print(f"▶ {title}")
    print("="*80)
    try:
        out = func()
        print("✔ SUCESSO")
        print("→ Resultado:", out)
    except Exception as e:
        print("✖ ERRO:", repr(e))
        print("-"*80)
        print(traceback.format_exc())

def main():
    # 1) Importa o LedgerService
    try:
        from services.ledger.service_ledger import LedgerService
    except Exception:
        print("✖ Não consegui importar LedgerService. Stack:")
        print(traceback.format_exc())
        sys.exit(1)

    L = LedgerService(DB_PATH)
    print("LedgerService criado:", L)
    print("Tem registrar_lancamento? ", hasattr(L, "registrar_lancamento"))
    print("Tem registrar_saida_boleto?", hasattr(L, "registrar_saida_boleto"))
    print("Tem pagar_parcela_boleto?", hasattr(L, "pagar_parcela_boleto"))

    # ===================== CASO A: Saída avulsa 'BOLETO' =====================
    # Simula seu formulário:
    # Data: 02/09/2025 | Valor: 400,00 | Forma: BOLETO | Categoria: Outros | Sub: Outros
    # Origem não vem do form -> o mixin infere 'Caixa'
    def case_a():
        return L.registrar_lancamento(
            tipo_evento="SAIDA",
            categoria_evento="Outros",
            subcategoria_evento="Outros",
            valor_evento="400,00",             # com vírgula
            forma="BOLETO",                     # será inferido para DINHEIRO
            descricao="teste avulsa BOLETO",
            data_evento="2025-09-02",          # aceita DD/MM/YYYY também
            # origem não informado: dispatcher vai usar "Caixa"
        )

    # ===================== CASO B: Saída via PIX =====================
    def case_b():
        return L.registrar_lancamento(
            tipo_evento="SAIDA",
            categoria_evento="Outros",
            subcategoria_evento="Outros",
            valor_evento=150.75,               # float puro
            forma="PIX",
            banco="Banco 1",                   # exigido para PIX/DÉBITO
            descricao="teste PIX",
            data_evento="2025-09-02",
        )

    # ===================== CASO C: Pagamento de boleto por obrigacao_id ======
    # Deve delegar ao serviço de boleto; se não houver parcelas, retorna mensagem amigável.
    def case_c():
        return L.registrar_saida_boleto(
            valor=123.45,
            forma="PIX",                       # aceito mas não altera CAP
            obrigacao_id=999999,               # ID que provavelmente não existe
            usuario="tester",
            data_evento="2025-09-02",
            descricao="pagto boleto inexistente (teste)",
        )

    # Rode os testes:
    run_case("CASO A: Saída avulsa 'BOLETO' (espera usar DINHEIRO/Caixa)", case_a)
    run_case("CASO B: Saída via PIX (Banco 1)", case_b)
    run_case("CASO C: Pagamento de boleto via obrigacao_id (sem parcelas)", case_c)

if __name__ == "__main__":
    main()
