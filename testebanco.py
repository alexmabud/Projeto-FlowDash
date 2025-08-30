import os, shutil, sqlite3, datetime, glob, sys

# 🔧 AJUSTE AQUI o caminho do seu banco:
db_path = r"C:\Users\User\OneDrive\Documentos\Python\Dev_Python\Abud Python Workspace - GitHub\Projeto FlowDash\data\flowdash_data.db"
# Se o atual for outro (ex.: dashboard_rc.db), troque na linha acima.

# 1) Preparar backup bruto (db + wal + shm se existirem)
ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
backup_dir = os.path.join(os.path.dirname(db_path), f"backup_corrupcao_{ts}")
os.makedirs(backup_dir, exist_ok=True)

base = os.path.splitext(db_path)[0]
candidates = [db_path, base + ".db-wal", base + ".db-shm"]  # nomes comuns
# também copia arquivos com mesmo prefixo (caso nome não termine em .db)
prefix = db_path
for f in glob.glob(prefix + "*"):
    if os.path.isfile(f) and os.path.basename(f) not in [os.path.basename(p) for p in candidates]:
        candidates.append(f)

copied = []
for f in candidates:
    if os.path.exists(f) and os.path.isfile(f):
        dest = os.path.join(backup_dir, os.path.basename(f))
        shutil.copy2(f, dest)
        copied.append(dest)

print("✅ Backup bruto salvo em:", backup_dir)
print("Arquivos copiados:", [os.path.basename(x) for x in copied])

# 2) Rodar PRAGMA integrity_check e quick_check
#    Abrimos em read-only para não alterar nada
uri = f"file:{db_path}?mode=ro"
try:
    con = sqlite3.connect(uri, uri=True, timeout=10)
    cur = con.cursor()
    cur.execute("PRAGMA integrity_check;")
    ic = cur.fetchone()[0]
    cur.execute("PRAGMA quick_check;")
    qc = cur.fetchone()[0]
    con.close()
    print("PRAGMA integrity_check =>", ic)
    print("PRAGMA quick_check   =>", qc)
except Exception as e:
    print("⚠️ Falha ao abrir/verificar o banco (modo leitura). Erro:", e)
    sys.exit(1)
