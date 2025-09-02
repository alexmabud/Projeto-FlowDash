# tests_check_ids.py
from __future__ import annotations
import os, sys, inspect

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

print("== Verificando módulo shared.ids em tempo de execução ==")
try:
    import shared.ids as ids
except Exception as e:
    print("ERRO importando shared.ids:", repr(e))
    sys.exit(1)

def show(fn):
    try:
        src = inspect.getsource(fn)
    except OSError:
        src = "<source indisponível>"
    print(f"\n--- {fn.__name__} ---")
    print(src[:500] + ("..." if len(src) > 500 else ""))
    try:
        print("Teste sanitize(4):", ids.sanitize(4))
    except Exception as e:
        print("sanitize(4) falhou:", repr(e))
    try:
        print("Teste sanitize_plus(4):", ids.sanitize_plus(4))
    except Exception as e:
        print("sanitize_plus(4) falhou:", repr(e))
    print("-"*60)

show(ids.sanitize)
show(ids.sanitize_plus)

print("\n== Testando UIDs com inteiros ==")
try:
    uid_bol = ids.uid_boleto_programado("2025-09-02", 400, 4, "2025-09-02", "Outros", "Outros", "desc", "user")
    print("uid_boleto_programado OK:", uid_bol[:20], "…")
except Exception as e:
    print("uid_boleto_programado falhou:", repr(e))

try:
    uid_din = ids.uid_saida_dinheiro("2025-09-02", 400, "Caixa", "Outros", "Outros", "desc", "user")
    print("uid_saida_dinheiro OK:", uid_din[:20], "…")
except Exception as e:
    print("uid_saida_dinheiro falhou:", repr(e))

try:
    uid_ban = ids.uid_saida_bancaria("2025-09-02", 400, "Banco 1", "PIX", "Outros", "Outros", "desc", "user")
    print("uid_saida_bancaria OK:", uid_ban[:20], "…")
except Exception as e:
    print("uid_saida_bancaria falhou:", repr(e))

print("\n== Caminho do módulo carregado ==")
print(ids.__file__)
