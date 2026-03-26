# 🚀 Databricks ERP AI & Data Dashboard

![Banner do Projeto](./assets/banner.png) <!-- Recomendo criar uma pasta assets e salvar o print longo do dashboard + chat aqui -->

Um sistema completo de ERP inteligente alimentado por **Databricks Data Intelligence Platform** e **Google Gemini 3.0 Flash**. 

Este projeto integra um Dashboard Analítico avançado com um Assistente de Inteligência Artificial Híbrido (RAG + Data Analytics), capaz de analisar tabelas consolidadas em tempo real e extrair insights precisos em Texto e Gráficos da sua base de dados Lakehouse.

---

## ✨ Principais Funcionalidades

- **📈 Dashboard Gerencial (Tempo Real):** Painel interativo consumindo dados consolidados (Gold) através da API, renderizando O Crescimento de Receita Mensal, Transações, Ticket Médio e Top Rank Produtos e Clientes.
- **🤖 Assistente IA com Hybrid-RAG:**
  - **Consultas Matemáticas Exatas:** O LLM responde perfeitamente a cálculos complexos e rankings recebendo dados determinísticos diretos das tabelas Gold do **Databricks SQL Warehouse**. Assim ele evita "alucinar" números e cálculos.
  - **Pesquisa Semântica (Vector Search):** Busca baseada em linguagem natural indexando toda a documentação com **Databricks Vector Search** e API endpoints.
- **📊 Geração de Gráficos pelo Chat:** A IA é capaz de estruturar respostas retornando JSON dinâmicos nativos do Plotly na própria interface de chat.
- **⬇️ Exportação de Relatórios (CSV):** Download via front-end (em um clique) de dados contábeis limpos e compatíveis com Excel (separados em ponto-e-vírgula e vírgula contábil) em `.csv` gerados a partir da visão da IA.
- **📚 Rastreio de Fontes Limpas (Citações):** A interface mostra quais documentos do Unity Catalog ou SQL Warehouse atestaram a veracidade da resposta.

---

## 🛠️ Arquitetura e Tecnologias

O sistema inteiro é sustentado por 3 grandes pilares: Engenharia de Dados (Lakehouse), Backend de Inteligência, e Frontend Responsivo.

### 1. 🗄️ Modelagem de Dados (Databricks Lakehouse)
- **PySpark:** Pipelines ETL rigorosos da Arquitetura Medalhão (Bronze -> Silver -> Gold). Consolidação absoluta da regra de negócios da empresa.
- **Databricks SQL Warehouse:** Motor em backend fornecedor de latência ultra-baixa de dados numéricos validados das camadas Ouro.
- **Databricks Vector Search:** Construtor de embeddings da Databricks em sincronia automática com atualizações de Catálogo (RAG puro).
- **Unity Catalog:** Governança total do ecossistema.

### 2. 🧠 Backend de Tráfego de Conhecimento (Python)
- **FastAPI / Uvicorn:** API RESTful robusta ligando o Frontend ao Lakehouse Databricks garantindo assincronismo.
- **Google Generative AI (Gemini 3 Flash):** LLM moderno capaz de ingerir dados nativos, formatar saídas textuais complexas, formatar gráficos multi-eixo, e manter memória de histórico rápida e barata.
- **Databricks SDK Workspace:** Automação de serviços em nuvem Databricks MLflow.
- **Pydantic:** Validação e parsing forte dos dicionários Python de entrada/saída.

### 3. 🖥️ Frontend de Apresentação Gerencial (React)
- **React 18 + Vite:** SPA fluida e polida projetada arquitetando uma UX focada em Dark Theme e elegância.
- **Tailwind CSS:** Flexibilidade customizada e paleta nativa elegante para estilos e tabelas.
- **Plotly.JS / React-Plotly:** Motor renderizador analítico no client-side com custom stacks em R$ Reais, e tradutor automático de datas.
- **Lucide React:** Iconografia refinada no sistema e cards.

---

## ⚙️ Como Executar o Projeto Localmente

### 1. Clone o Repositório
```bash
git clone https://github.com/SeuUsuario/project-databricks-rag.git
cd project-databricks-rag
```

### 2. Configurando o Backend (API) e Access Tokens
Navegue até a pasta `/api` e garanta que o arquivo `.env` tenha as chaves cadastradas:
```bash
cd api
```
Crie seu arquivo `.env`:
```text
DATABRICKS_HOST=https://seu-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
DATABRICKS_SQL_WAREHOUSE_ID=xxxxxxx
GEMINI_API_KEY=AIZaSy...
```
Inicie a aplicação FastAPI localmente:
```bash
python main.py
```
*(Opcional: instale as dependências com `pip install -r requirements.txt`, garantindo acesso ao `google-generativeai`, `fastapi`, `databricks-sdk`, `uvicorn` e `pyspark`)*. A API rodará em `http://localhost:8000`.

### 3. Subindo o Frontend Gerencial
Abra uma segunda aba no terminal, navegue para `/frontend` e rode a build em desenvolvimento:
```bash
cd frontend
npm install
npm run dev
```
A aplicação abrirá no endereço local `http://localhost:5173`. Aproveite sua assistente RAG integrada com base local!

---

## 📸 Demonstração do Projeto

**(Sua Imagem 1 Aqui)**
> *Recomendação: `![Visão Geral do Dashboard - KPI + Gráficos](./assets/dash.png)`*

**(Sua Imagem 2 Aqui)**
> *Recomendação: `![RAG Híbrido, Botão CSV e Tooltips Formatos em R$](./assets/chat_rag.png)`*

---
<div align="center">
  <i>Desenvolvido e Otimizado visando robustez contábil e alta confiabilidade de Dados na era da Inteligência Artificial.</i>
</div>
