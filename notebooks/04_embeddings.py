# Databricks notebook source

# ============================================================================
# ETAPA 4 — GERAÇÃO DE EMBEDDINGS
# ============================================================================
# Objetivo: Transformar dados Gold em textos descritivos e gerar embeddings
#           para busca vetorial usando Databricks Foundation Model APIs.
#
# Entrada: Tabelas Delta Gold
# Saída:   Tabela de documentos + índice Databricks Vector Search
#
# Tecnologias:
#   - Databricks Foundation Model APIs (BGE embeddings)
#   - Databricks Vector Search
# ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧠 Geração de Embeddings
# MAGIC
# MAGIC Converte dados estruturados da camada Gold em textos descritivos
# MAGIC e gera embeddings vetoriais para consulta via RAG.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, FloatType
import mlflow.deployments

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configurações

# COMMAND ----------

CATALOG_NAME = "workspace"
GOLD_SCHEMA = "gold_ecommerce"

# Tabelas Gold (entrada)
GOLD_REVENUE_SUMMARY = f"{CATALOG_NAME}.{GOLD_SCHEMA}.revenue_summary"
GOLD_TOP_CUSTOMERS = f"{CATALOG_NAME}.{GOLD_SCHEMA}.top_customers"
GOLD_TOP_PRODUCTS = f"{CATALOG_NAME}.{GOLD_SCHEMA}.top_products"
GOLD_KPIS = f"{CATALOG_NAME}.{GOLD_SCHEMA}.kpis"

# Tabela de documentos para embeddings
EMBEDDINGS_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.embeddings_documents"

# Databricks Foundation Model - Embedding endpoint
EMBEDDING_ENDPOINT = "databricks-bge-large-en"

# Vector Search
VS_ENDPOINT_NAME = "ecommerce_rag_vs_endpoint"
VS_INDEX_NAME = f"{CATALOG_NAME}.{GOLD_SCHEMA}.embeddings_vs_index"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Geração de Textos Descritivos
# MAGIC
# MAGIC Converte cada registro das tabelas Gold em textos descritivos
# MAGIC que serão usados para gerar embeddings.

# COMMAND ----------

def format_currency(value):
    """Formata valor como moeda brasileira."""
    if value is None:
        return "R$ 0,00"
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# COMMAND ----------

# --- 1. Textos dos Top Clientes ---
df_customers = spark.table(GOLD_TOP_CUSTOMERS)

customer_docs = []
for row in df_customers.collect():
    text = (
        f"Cliente {row['customer_name']} da cidade {row['city']}, "
        f"estado {row['state']}. "
        f"Realizou {row['total_orders']} pedidos com receita total de "
        f"{format_currency(row['total_revenue'])}. "
        f"Ticket médio de {format_currency(row['avg_ticket'])}. "
        f"Primeiro pedido em {row['first_order_date']} e último em {row['last_order_date']}. "
        f"Ranking de receita: #{row['rank']}."
    )
    customer_docs.append({
        "doc_id": f"customer_{row['customer_id']}",
        "doc_type": "customer",
        "text": text
    })

print(f"✅ Gerados {len(customer_docs)} documentos de clientes")

# COMMAND ----------

# --- 2. Textos dos Top Produtos ---
df_products = spark.table(GOLD_TOP_PRODUCTS)

product_docs = []
for row in df_products.collect():
    text = (
        f"Produto '{row['product_name']}' da categoria '{row['category']}'. "
        f"Preço médio de {format_currency(row['avg_unit_price'])}. "
        f"Total vendido: {row['total_quantity']} unidades em {row['num_orders']} pedidos. "
        f"Receita total gerada: {format_currency(row['total_revenue'])}. "
        f"Ranking por receita: #{row['rank_revenue']}. "
        f"Ranking por quantidade: #{row['rank_quantity']}."
    )
    product_docs.append({
        "doc_id": f"product_{row['product_id']}",
        "doc_type": "product",
        "text": text
    })

print(f"✅ Gerados {len(product_docs)} documentos de produtos")

# COMMAND ----------

# --- 3. Textos de Receita Mensal ---
df_revenue = spark.table(GOLD_REVENUE_SUMMARY)

revenue_docs = []
for row in df_revenue.collect():
    text = (
        f"No período {row['order_year_month']}: "
        f"receita total de {format_currency(row['total_revenue'])} "
        f"com {row['total_orders']} pedidos realizados. "
        f"Ticket médio de {format_currency(row['avg_ticket'])}. "
        f"Menor pedido: {format_currency(row['min_order_value'])}. "
        f"Maior pedido: {format_currency(row['max_order_value'])}. "
        f"Clientes únicos: {row['unique_customers']}. "
        f"Crescimento em relação ao mês anterior: {row['growth_pct']}%."
    )
    revenue_docs.append({
        "doc_id": f"revenue_{row['order_year_month']}",
        "doc_type": "revenue",
        "text": text
    })

print(f"✅ Gerados {len(revenue_docs)} documentos de receita mensal")

# COMMAND ----------

# --- 4. Textos de KPIs ---
df_kpis = spark.table(GOLD_KPIS)

kpi_docs = []
for row in df_kpis.collect():
    text = (
        f"KPI do tipo '{row['kpi_type']}' para '{row['dimension_value']}': "
        f"{row['total_orders']} pedidos com receita total de "
        f"{format_currency(row['total_revenue'])} e "
        f"ticket médio de {format_currency(row['avg_ticket'])}."
    )
    kpi_docs.append({
        "doc_id": f"kpi_{row['kpi_type']}_{row['dimension_value']}",
        "doc_type": "kpi",
        "text": text
    })

print(f"✅ Gerados {len(kpi_docs)} documentos de KPIs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Criação da Tabela de Documentos

# COMMAND ----------

# Consolida todos os documentos
all_docs = customer_docs + product_docs + revenue_docs + kpi_docs

print(f"\n📊 Total de documentos gerados: {len(all_docs)}")
print(f"  • Clientes: {len(customer_docs)}")
print(f"  • Produtos: {len(product_docs)}")
print(f"  • Receita Mensal: {len(revenue_docs)}")
print(f"  • KPIs: {len(kpi_docs)}")

# Cria DataFrame com os documentos
df_docs = (
    spark.createDataFrame(all_docs)
    .withColumn("_created_timestamp", F.current_timestamp())
)

# Salva como tabela Delta (necessária para o Vector Search)
df_docs.write.format("delta").mode("overwrite").saveAsTable(EMBEDDINGS_TABLE)

print(f"\n✅ Tabela {EMBEDDINGS_TABLE} criada com {df_docs.count()} documentos")
df_docs.show(5, truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Configuração do Databricks Vector Search
# MAGIC
# MAGIC O Vector Search do Databricks automaticamente:
# MAGIC 1. Gera embeddings dos textos usando o modelo especificado
# MAGIC 2. Indexa os embeddings para busca vetorial
# MAGIC 3. Mantém o índice sincronizado com a tabela Delta

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

# Inicializa o cliente Vector Search
vsc = VectorSearchClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criar Endpoint de Vector Search (se não existir)

# COMMAND ----------

# Lista endpoints existentes para verificar
try:
    endpoints = vsc.list_endpoints()
    endpoint_names = [e["name"] for e in endpoints.get("endpoints", [])]

    if VS_ENDPOINT_NAME not in endpoint_names:
        print(f"🔧 Criando endpoint: {VS_ENDPOINT_NAME}")
        vsc.create_endpoint(
            name=VS_ENDPOINT_NAME,
            endpoint_type="STANDARD"
        )
        print(f"✅ Endpoint {VS_ENDPOINT_NAME} criado com sucesso!")
    else:
        print(f"✅ Endpoint {VS_ENDPOINT_NAME} já existe.")
except Exception as e:
    print(f"⚠️ Erro ao verificar/criar endpoint: {e}")
    print("  Você pode criar o endpoint manualmente no Databricks UI:")
    print(f"  Compute > Vector Search > Create Endpoint > Nome: {VS_ENDPOINT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criar Índice de Vector Search

# COMMAND ----------

# Cria (ou recria) o índice vetorial
# O Delta Sync Index sincroniza automaticamente com a tabela Delta
try:
    # Tenta deletar índice existente
    try:
        vsc.delete_index(
            endpoint_name=VS_ENDPOINT_NAME,
            index_name=VS_INDEX_NAME
        )
        print(f"🗑️ Índice anterior {VS_INDEX_NAME} removido.")
    except Exception:
        pass  # Índice não existia

    # Cria novo índice com Delta Sync
    index = vsc.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT_NAME,
        index_name=VS_INDEX_NAME,
        source_table_name=EMBEDDINGS_TABLE,
        pipeline_type="TRIGGERED",
        primary_key="doc_id",
        embedding_source_column="text",
        embedding_model_endpoint_name=EMBEDDING_ENDPOINT
    )

    print(f"✅ Índice {VS_INDEX_NAME} criado com sucesso!")
    print(f"  • Endpoint: {VS_ENDPOINT_NAME}")
    print(f"  • Modelo de embedding: {EMBEDDING_ENDPOINT}")
    print(f"  • Coluna de texto: text")
    print(f"  • Chave primária: doc_id")
    print(f"  • Tipo de pipeline: TRIGGERED")

except Exception as e:
    print(f"⚠️ Erro ao criar índice: {e}")
    print("\n  Alternativa manual:")
    print(f"  1. Vá em Compute > Vector Search")
    print(f"  2. Selecione o endpoint: {VS_ENDPOINT_NAME}")
    print(f"  3. Crie um novo índice Delta Sync:")
    print(f"     - Source Table: {EMBEDDINGS_TABLE}")
    print(f"     - Primary Key: doc_id")
    print(f"     - Embedding Source: text")
    print(f"     - Embedding Model: {EMBEDDING_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Validações

# COMMAND ----------

print("=" * 60)
print("  📋 VALIDAÇÕES DA ETAPA DE EMBEDDINGS")
print("=" * 60)

# Verifica tabela de documentos
df_check = spark.table(EMBEDDINGS_TABLE)
total_docs = df_check.count()
doc_types = df_check.groupBy("doc_type").count().collect()

print(f"\n  📄 Total de documentos: {total_docs}")
for row in doc_types:
    print(f"    • {row['doc_type']}: {row['count']} documentos")

# Verifica status do índice
try:
    index_info = vsc.get_index(
        endpoint_name=VS_ENDPOINT_NAME,
        index_name=VS_INDEX_NAME
    )
    status = index_info.describe()
    print(f"\n  🔍 Status do índice: {status.get('status', {}).get('ready', 'UNKNOWN')}")
except Exception as e:
    print(f"\n  ⚠️ Não foi possível verificar o índice: {e}")

print("\n" + "=" * 60)
print("  🎉 ETAPA DE EMBEDDINGS CONCLUÍDA!")
print("=" * 60)
