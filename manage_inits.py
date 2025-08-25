from pathlib import Path

ROOT = Path(__file__).parent

# Pastas onde o __init__.py é obrigatório
REQUIRED = [
    "utils",
    "shared",
    "services",
    "repository",
    "flowdash_pages",
    "flowdash_pages/lancamentos",
    "flowdash_pages/lancamentos/caixa2",
    "flowdash_pages/lancamentos/deposito",
    "flowdash_pages/lancamentos/mercadorias",
    "flowdash_pages/lancamentos/pagina",
    "flowdash_pages/lancamentos/saida",
    "flowdash_pages/lancamentos/transferencia",
    "flowdash_pages/lancamentos/venda",
    "services/ledger",
    "repository/contas_a_pagar_mov_repository",
]

# Pastas onde é opcional (criamos __init__ só se existirem)
OPTIONAL = [
    "cadastros",
    "dashboard",
    "dre",
    "fechamento",
    "banco",
    "metas",
    "flowdash_pages/cadastros",
    "flowdash_pages/dashboard",
    "flowdash_pages/dre",
    "flowdash_pages/fechamento",
    "flowdash_pages/dataframes",
]

def ensure_init(dirpath: Path, content: str = "# package marker\n"):
    dirpath.mkdir(parents=True, exist_ok=True)
    init_file = dirpath / "__init__.py"
    if not init_file.exists():
        init_file.write_text(content, encoding="utf-8")
        print(f"[OK] Criado: {init_file}")
    else:
        print(f"[=] Já existe: {init_file}")

def list_subpackages(dirpath: Path):
    return sorted([d.name for d in dirpath.iterdir() if d.is_dir() and (d / "__init__.py").exists()])

def list_modules(dirpath: Path):
    return sorted([p.stem for p in dirpath.glob("*.py") if p.name != "__init__.py"])

def write_all_init(dirpath: Path, names: list[str], header: str = ""):
    init_file = dirpath / "__init__.py"
    lines = []
    if header:
        lines.append(header.rstrip() + "\n")
    lines.append("__all__ = [\n")
    for name in names:
        lines.append(f"    '{name}',\n")
    lines.append("]\n")
    init_file.write_text("".join(lines), encoding="utf-8")
    print(f"[OK] __all__ atualizado: {init_file}")

def main():
    # 1) Criar __init__.py obrigatórios (vazios)
    for rel in REQUIRED:
        ensure_init(ROOT / rel)

    # 2) Criar __init__.py opcionais se pasta existir
    for rel in OPTIONAL:
        p = ROOT / rel
        if p.exists() and p.is_dir():
            ensure_init(p)

    # 3) __all__ automático para flowdash_pages/lancamentos (subPACOTES)
    lanc = ROOT / "flowdash_pages" / "lancamentos"
    if lanc.exists():
        subs = list_subpackages(lanc)
        if subs:
            write_all_init(
                lanc,
                subs,
                header="# auto-generated exports for subpackages of flowdash_pages.lancamentos"
            )

    # 4) __all__ automático para services/ledger (subMÓDULOS .py)
    ledger = ROOT / "services" / "ledger"
    if ledger.exists():
        mods = list_modules(ledger)
        if mods:
            write_all_init(
                ledger,
                mods,
                header="# auto-generated exports for modules in services.ledger"
            )

    # 5) __all__ automático para repository/contas_a_pagar_mov_repository (subMÓDULOS .py)
    cap_repo = ROOT / "repository" / "contas_a_pagar_mov_repository"
    if cap_repo.exists():
        mods = list_modules(cap_repo)
        if mods:
            write_all_init(
                cap_repo,
                mods,
                header="# auto-generated exports for modules in repository.contas_a_pagar_mov_repository"
            )

if __name__ == "__main__":
    main()