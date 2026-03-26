# ============================================================================
# Configurações do Projeto - Data Pipeline + RAG com Databricks
# ============================================================================
# Este módulo centraliza todas as configurações do projeto.
# Ajuste os valores conforme seu ambiente Databricks.
# ============================================================================

import os

# ----------------------------------------------------------------------------
# CATÁLOGO E SCHEMA (Unity Catalog)
# ----------------------------------------------------------------------------
CATALOG_NAME = "workspace"
BRONZE_SCHEMA = "bronze_ecommerce"
SILVER_SCHEMA = "silver_ecommerce"
GOLD_SCHEMA = "gold_ecommerce"

# ----------------------------------------------------------------------------
# CAMINHOS DAS TABELAS DELTA (por camada)
# ----------------------------------------------------------------------------
# Tabelas Bronze
BRONZE_CUSTOMERS = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.customers"
BRONZE_PRODUCTS = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.products"
BRONZE_ORDERS = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.orders"

# Tabelas Silver
SILVER_CUSTOMERS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.customers"
SILVER_PRODUCTS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.products"
SILVER_ORDERS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.orders"

# Tabelas Gold
GOLD_REVENUE_SUMMARY = f"{CATALOG_NAME}.{GOLD_SCHEMA}.revenue_summary"
GOLD_TOP_CUSTOMERS = f"{CATALOG_NAME}.{GOLD_SCHEMA}.top_customers"
GOLD_TOP_PRODUCTS = f"{CATALOG_NAME}.{GOLD_SCHEMA}.top_products"
GOLD_KPIS = f"{CATALOG_NAME}.{GOLD_SCHEMA}.kpis"

# Tabela de Embeddings
EMBEDDINGS_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.embeddings_documents"

# ----------------------------------------------------------------------------
# CAMINHOS DE DADOS LOCAIS
# ----------------------------------------------------------------------------
DATA_RAW_PATH = "data/raw"
DATA_PROCESSED_PATH = "data/processed"

# Arquivos JSON de entrada
RAW_CUSTOMERS_FILE = f"{DATA_RAW_PATH}/customers.json"
RAW_PRODUCTS_FILE = f"{DATA_RAW_PATH}/products.json"
RAW_ORDERS_FILE = f"{DATA_RAW_PATH}/orders.json"

# ----------------------------------------------------------------------------
# DATABRICKS FOUNDATION MODEL APIs
# ----------------------------------------------------------------------------
# Endpoint para geração de texto (LLM)
LLM_ENDPOINT_NAME = "databricks-meta-llama-3-1-70b-instruct"

# Endpoint para geração de embeddings
EMBEDDING_ENDPOINT_NAME = "databricks-bge-large-en"

# Dimensão dos embeddings (BGE-large = 1024)
EMBEDDING_DIMENSION = 1024

# ----------------------------------------------------------------------------
# DATABRICKS VECTOR SEARCH
# ----------------------------------------------------------------------------
VECTOR_SEARCH_ENDPOINT_NAME = "ecommerce_rag_vs_endpoint"
VECTOR_SEARCH_INDEX_NAME = f"{CATALOG_NAME}.{GOLD_SCHEMA}.embeddings_vs_index"

# Número de resultados retornados na busca vetorial
VECTOR_SEARCH_TOP_K = 5

# ----------------------------------------------------------------------------
# CONFIGURAÇÕES DO RAG
# ----------------------------------------------------------------------------
RAG_SYSTEM_PROMPT = """Você é um assistente de análise de dados de e-commerce.
Responda perguntas com base EXCLUSIVAMENTE nos dados fornecidos no contexto abaixo.
Se a informação não estiver disponível no contexto, diga que não possui dados suficientes.
Sempre forneça números e métricas quando disponíveis.
Responda sempre em português brasileiro."""

RAG_MAX_TOKENS = 1024
RAG_TEMPERATURE = 0.1

# ----------------------------------------------------------------------------
# LOGGING
# ----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
