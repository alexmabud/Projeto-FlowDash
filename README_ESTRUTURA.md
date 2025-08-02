
# 📁 Estrutura de Pastas — FlowDash

Este arquivo descreve a estrutura organizacional do projeto **FlowDash**, com explicações sobre o propósito de cada pasta e arquivo, conforme a estrutura modular real utilizada.

---

## 🌳 Estrutura Atual

```
FlowDash/
│
├── main.py
├── lancamentos.py
│
├── auth/
│   └── auth.py
│
├── banco/
│   └── banco.py
│
├── cadastro/
│   └── cadastro.py
│
├── dashboard/
│   └── dashboard.py
│
├── services/
│   └── (regras de negócio: ex. comissões, metas, validações)
│
├── ui/
│   └── ui.py
│
├── utils/
│   └── utils.py
│
├── data/
│   └── flowdash_data.db
│
├── fluxograma/
│   └── Fluxograma FlowDash.png
│
├── README.md
└── README_ESTRUTURA.md
```

---

## 🗂️ Detalhamento das Pastas e Arquivos

| Caminho                    | Descrição                                                                 |
|----------------------------|---------------------------------------------------------------------------|
| `main.py`                  | Ponto de entrada principal. Define a estrutura de navegação do app.       |
| `lancamentos.py`           | Tela principal com login, menu lateral e funcionalidades integradas.      |
| `auth/auth.py`             | Lógica de login, controle de sessão, perfis e acesso por usuário.         |
| `banco/banco.py`           | Conexão com o SQLite e funções de leitura de todas as tabelas do sistema. |
| `cadastro/cadastro.py`     | Telas para cadastro de usuários, metas, taxas, cartões, saldos etc.       |
| `dashboard/dashboard.py`   | KPIs, gráficos de metas, vendas e indicadores do painel.                  |
| `services/`                | Pasta reservada para lógica de negócio (ex: comissão por meta).           |
| `ui/ui.py`                 | Componentes visuais customizados (estilização, botões, blocos HTML).      |
| `utils/utils.py`           | Funções auxiliares: formatação, dias úteis, senha forte, hash etc.        |
| `data/flowdash_data.db`    | Banco de dados SQLite contendo as tabelas da aplicação.                   |
| `fluxograma/`              | Diagrama do fluxo da aplicação em imagem (`Fluxograma FlowDash.png`).     |
| `README.md`                | Apresentação geral do projeto, funcionalidades, instalação.               |
| `README_ESTRUTURA.md`      | (este arquivo) Detalhamento técnico da estrutura de arquivos e pastas.    |

---

## 💡 Observações

- A estrutura foi planejada para:
  - Organização modular por responsabilidade
  - Facilidade de manutenção e expansão
  - Reutilização de partes do sistema em outros contextos
  - Migração futura para frameworks como Django, Flask ou interfaces desktop
- Todas as funções e lógicas estão agrupadas por tema: banco, autenticação, interface, utilidades e regras de negócio.
- O banco de dados (`flowdash_data.db`) está isolado na pasta `data/` por organização.
- O fluxograma visual serve como guia funcional do sistema.

---

**Autor:** Alex Abud  
**Projeto:** FlowDash – Sistema de Fluxo de Caixa + Dashboard Inteligente
