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

| Pasta / Arquivo         | Descrição                                                                 |
|--------------------------|--------------------------------------------------------------------------|
| `main.py`               | Ponto de entrada principal do sistema.                                   |
| `lancamentos.py`        | Interface principal: login, menu lateral e telas integradas.             |
| `auth/`                 | Lógica de login, controle de sessão e verificação de perfil.             |
| `banco/`                | Funções de acesso ao banco SQLite (leitura das tabelas).                 |
| `cadastro/`             | Telas e funcionalidades para cadastro de usuários, metas, saldos etc.    |
| `dashboard/`            | Geração de gráficos e indicadores financeiros.                           |
| `services/`             | Regras de negócio reutilizáveis (ex: cálculo de comissão, metas).        |
| `ui/`                   | Componentes visuais e estilizações personalizadas com Streamlit.         |
| `utils/`                | Funções auxiliares (formatação, datas úteis, hash de senha etc.).        |
| `data/flowdash_data.db` | Banco de dados SQLite com as tabelas do sistema (ignorado no GitHub por conter dados reais). |
| `data/flowdash_template.db` | Banco de dados vazio (somente esquema + usuário admin padrão).        |
| `fluxograma/`           | Contém o diagrama do fluxo da aplicação (`Fluxograma FlowDash.png`).     |
| `README.md`             | Apresentação geral do projeto (este arquivo).                            |
| `README_ESTRUTURA.md`   | Explicação técnica da estrutura de pastas e organização do sistema.      |

---

## ✅ Funcionalidades Implementadas

- **Login com controle de perfil**:
  - Administrador
  - Gerente
  - Vendedor
- **Lançamentos do Dia**:
  - Cadastro de entradas
  - Cadastro de saídas
  - Transferência entre Caixas
- **Cadastro**:
  - Usuários com ativação/desativação
  - Taxas de maquininhas por forma, bandeira e parcelas
  - Cartões de crédito (vencimento e fechamento)
  - Saldos bancários e de caixa
  - Metas por vendedor (diária, semanal, mensal e por nível)
- **Fechamento de Caixa**:
  - Entradas confirmadas (com taxas aplicadas)
  - Saldo final esperado por caixa e banco
  - Correções manuais e controle de saldos acumulados
- **Dashboard**: em construção  
- **DRE (Demonstrativo de Resultado)**: em construção

---

## 🔐 Segurança

- Senhas dos usuários são protegidas com **hash SHA-256** usando `hashlib`
- Validação de senha forte com letras, números e símbolos
- Controle de acesso baseado em perfil (restrição por seção)

---

## 🛠️ Tecnologias Utilizadas

- **Linguagem:** Python 3.12+
- **Interface:** Streamlit
- **Banco de Dados:** SQLite3
- **Visualizações:** Plotly / Matplotlib
- **Manipulação de Dados:** Pandas
- **Calendário de Feriados:** Workalendar (com suporte ao DF)
- **Criptografia de Senhas:** hashlib (SHA-256)
- **Outras:** datetime, os, re

---

## 📦 Bibliotecas e Dependências

As dependências externas estão listadas no arquivo `requirements.txt`.

Principais bibliotecas utilizadas:

- `streamlit` → Interface gráfica interativa  
- `pandas` → Manipulação de dados financeiros  
- `plotly` → Geração de gráficos interativos  
- `matplotlib` → Visualizações complementares  
- `workalendar` → Cálculo de dias úteis e feriados regionais (DF)

---

## 📝 Banco de Dados

- Arquivo real: `data/flowdash_data.db` (**ignorado no GitHub**, pois contém dados reais).  
- Arquivo modelo: `data/flowdash_template.db` (**incluso no repositório**, sem dados, apenas esquema e um usuário admin).  

**Credenciais padrão do template:**
- Usuário: `admin@local`
- Senha: `admin`

**Copiar o template para o banco ativo (obrigatório antes de rodar):**

Windows (CMD):
~~~bat
if not exist data mkdir data
copy /Y data\flowdash_template.db data\flowdash_data.db
~~~

Linux/Mac:
~~~bash
mkdir -p data
cp -f data/flowdash_template.db data/flowdash_data.db
~~~

---

## 🚀 Como Executar o Projeto

1. Clone o repositório:
   ~~~bash
   git clone https://github.com/seu-usuario/flowdash.git
   cd flowdash
   ~~~

2. Crie e ative um ambiente virtual (recomendado):
   ~~~bash
   conda create -n flowdash python=3.12 -y
   conda activate flowdash
   ~~~
   *(ou, se preferir venv: `python -m venv venv && venv\Scripts\activate` no Windows)*

3. Instale as dependências:
   ~~~bash
   pip install -r requirements.txt
   ~~~

4. Prepare o banco de dados (copie o template para o ativo):

Windows (CMD):
~~~bat
if not exist data mkdir data
copy /Y data\flowdash_template.db data\flowdash_data.db
~~~

Linux/Mac:
~~~bash
mkdir -p data
cp -f data/flowdash_template.db data/flowdash_data.db
~~~

5. Execute o sistema:
   ~~~bash
   streamlit run main.py
   ~~~

O navegador abrirá em `http://localhost:8501` (ou outra porta disponível).

---

## 👨‍💻 Autor

**Alex Abud**  
**Projeto:** FlowDash – Sistema de Fluxo de Caixa + Dashboard Inteligente
