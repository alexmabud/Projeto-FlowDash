# 📁 Estrutura de Pastas — FlowDash

Este arquivo descreve a estrutura organizacional do projeto **FlowDash**, com explicações sobre o propósito de cada pasta e arquivo.

---

## 🌳 Estrutura Proposta

```
FlowDash/
│
├── main.py
├── lancamentos.py
├── banco.py
├── ui.py
├── dashboard.py
├── cadastro.py
├── funcoes.py
│
├── data/
│   └── flowdash_data.db
│
├── fluxograma/
│   └── Fluxograma FlowDash.png
│
└── README.md
```

---

## 🗂️ Detalhamento

| Caminho / Arquivo         | Descrição                                                                 |
|---------------------------|---------------------------------------------------------------------------|
| `main.py`                 | Ponto de entrada da aplicação. Gerencia o fluxo principal em Streamlit.  |
| `lancamentos.py`          | Lógica principal da interface: login, menu lateral e funcionalidades.     |
| `banco.py`                | Conexão com o banco SQLite e funções de leitura/escrita.                  |
| `ui.py`                   | Componentes visuais customizados para a interface com Streamlit.          |
| `dashboard.py`            | Geração de gráficos e indicadores para o painel de controle.              |
| `cadastro.py`             | Telas e lógicas para cadastro de usuários, metas, taxas, etc.             |
| `funcoes.py`              | Funções auxiliares reutilizáveis (formatar valores, dias úteis, hash).    |
| `data/`                   | Contém o banco de dados SQLite (`flowdash_data.db`).                      |
| `fluxograma/`             | Imagem com o fluxograma visual do sistema.                                |
| `README.md`               | Descrição geral do projeto, instalação e funcionalidades.                 |
| `README_ESTRUTURA.md`     | (este arquivo) Explicação técnica da estrutura de pastas.                 |

---

## 💡 Observações

- A estrutura foi planejada para permitir **migração futura para Django, Flask ou Desktop (Tkinter/PyQt)**.
- A modularização facilita manutenção e escalabilidade do projeto.
- O banco de dados SQLite está separado em `data/` para organização.
- O arquivo do fluxograma (`.png`) pode ser aberto para entender o fluxo do sistema visualmente.

---

**Autor:** Alex Abud
**Projeto:** FlowDash – Sistema de Fluxo de Caixa + Dashboard Inteligente
