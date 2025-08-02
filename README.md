# 💼 FlowDash

Sistema completo de Fluxo de Caixa e Dashboard para controle financeiro de loja física, desenvolvido em **Python**, com interface gráfica em **Streamlit** e persistência de dados em **SQLite**.

---

## 🧠 Objetivo

Criar uma aplicação robusta, modular e escalável para controle financeiro, com foco em:

- Controle de entradas e saídas
- Metas e comissões por perfil
- Fechamento de caixa diário
- Visualização em dashboard
- Cadastro de usuários, taxas, cartões, bancos, metas, etc.

---

## 🗂️ Estrutura de Pastas

| Pasta / Arquivo     | Descrição                                                                 |
|---------------------|---------------------------------------------------------------------------|
| `main.py`           | Arquivo principal da aplicação (ponto de entrada do sistema)              |
| `lancamentos.py`    | Interface principal com lógica dos lançamentos e menus com Streamlit      |
| `data/`             | Contém o banco de dados SQLite (`flowdash_data.db`)                       |
| `banco.py`          | Módulo para conexão e operações com o banco de dados                      |
| `ui.py`             | Componentes visuais e estilizações personalizadas para Streamlit          |
| `dashboard.py`      | Geração de gráficos e indicadores financeiros                             |
| `cadastro.py`       | Telas e funções para cadastro de dados (usuários, metas, taxas, etc.)     |
| `funcoes.py`        | Funções utilitárias e auxiliares (validações, cálculos, formatações)      |
| `fluxograma/`       | Contém o arquivo do fluxograma do sistema (imagem para consulta interna)  |

---

## ✅ Funcionalidades Implementadas

- **Login com controle de perfil** (Administrador, Gerente, Vendedor)
- **Lançamentos do Dia**:
  - Cadastro de Entradas
  - Cadastro de Saídas
  - Transferência entre Caixa e Caixa 2
- **Cadastro**:
  - Usuários com status ativo/inativo
  - Taxas de maquininhas por forma de pagamento, bandeira e parcelas
  - Cartões de crédito (com vencimento e fechamento)
  - Saldos bancários e em caixa
  - Metas de vendas (diária, semanal, mensal e por nível)
- **Fechamento de Caixa**:
  - Entradas confirmadas (com taxas aplicadas)
  - Visualização do saldo em caixa e bancos
  - Correções manuais
- **Dashboard**: em construção
- **DRE**: em construção

---

## 🔐 Segurança

- **Hash de senhas:** as senhas dos usuários são armazenadas com `SHA-256` usando `hashlib`.

---

## 🛠️ Tecnologias Utilizadas

- **Linguagem:** Python 3.10+
- **Framework Web:** Streamlit
- **Banco de Dados:** SQLite3
- **Gráficos:** Plotly
- **Manipulação de Dados:** Pandas
- **Calendário de Feriados:** Workalendar (com suporte ao DF)
- **Hash de Senhas:** hashlib (SHA-256)
- **Outras:** datetime, os, re

---

## 📦 Bibliotecas e Dependências

Essas são as principais bibliotecas usadas no projeto:

| Biblioteca         | Tipo            | Finalidade                                                         |
|--------------------|------------------|----------------------------------------------------------------------|
| `streamlit`        | Externa          | Interface gráfica e menus interativos                               |
| `sqlite3`          | Nativa           | Conexão e manipulação do banco de dados SQLite                      |
| `os`               | Nativa           | Manipulação de diretórios e caminhos                                |
| `hashlib`          | Nativa           | Criptografia de senhas (SHA-256)                                    |
| `pandas`           | Externa          | Manipulação de tabelas, DataFrames, dados financeiros               |
| `re`               | Nativa           | Validação de senhas e expressões regulares                          |
| `datetime`         | Nativa           | Manipulação de datas e períodos                                     |
| `plotly.graph_objects` | Externa     | Geração de gráficos interativos                                     |
| `workalendar.america.BrazilDistritoFederal` | Externa | Cálculo de dias úteis e feriados do DF                              |

---

## 📝 Banco de Dados

- Arquivo: `data/flowdash_data.db`
- Principais Tabelas:
  - `entrada`, `saida`
  - `usuarios`, `taxas_maquinas`
  - `saldos_caixas`, `saldos_bancos`, `correcao_caixa`, `fechamento_caixa`
  - `metas`, `cartoes_credito`, `fatura_cartao`

---

## 🚀 Como Executar o Projeto

1. Instale os requisitos:
```bash
pip install -r requirements.txt
```

2. Execute o sistema:
```bash
streamlit run lancamentos.py
```

---

## 📁 Arquivos de Referência

- `fluxograma/Fluxograma FlowDash.png`: diagrama com o fluxo geral da aplicação
- `funcoes_completas.py`: conjunto de funções reutilizáveis (utils, auth, banco, dashboard etc.)

---

**Autor:** Alex Abud
**Projeto:** FlowDash – Sistema de Fluxo de Caixa + Dashboard Inteligente
