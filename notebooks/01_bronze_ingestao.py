# Databricks notebook source

# ============================================================================
# ETAPA 1 — CAMADA BRONZE (Ingestão de Dados Brutos)
# ============================================================================
# Objetivo: Capturar dados brutos sem transformação e persistir em Delta.
#
# Entrada: Arquivos JSON em data/raw/
# Saída:   Tabelas Delta Bronze (bronze_customers, bronze_products, bronze_orders)
# ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥉 Camada Bronze — Ingestão de Dados Brutos
# MAGIC
# MAGIC Este notebook realiza a ingestão dos dados brutos de e-commerce
# MAGIC (clientes, produtos e pedidos) e persiste em tabelas Delta sem tratamento.

# COMMAND ----------

# Imports
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, ArrayType
)
import sys
import os

# Adiciona o diretório raiz do projeto ao path
# (ajuste conforme a localização no seu workspace Databricks)
# sys.path.insert(0, "/Workspace/Users/seu_email/project_databricks_rag")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configurações

# COMMAND ----------

# Configurações (inline para facilitar uso como notebook Databricks)
CATALOG_NAME = "workspace"
BRONZE_SCHEMA = "bronze_ecommerce"

# Tabelas Bronze
BRONZE_CUSTOMERS = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.customers"
BRONZE_PRODUCTS = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.products"
BRONZE_ORDERS = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.orders"

# Caminho dos dados brutos (Unity Catalog Volume - preenchido pelo deploy.py)
DATA_RAW_PATH = "/Volumes/main/ecommerce_rag/raw_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Criação do Schema (Unity Catalog)

# COMMAND ----------

# Cria o catálogo e schema se não existirem
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{BRONZE_SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")

print(f"✅ Usando: {CATALOG_NAME}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Ingestão: Clientes

# COMMAND ----------

# Schema dos clientes
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("registration_date", StringType(), True)
])

# Leitura do JSON
df_customers_raw = (
    spark.read
    .option("multiline", "true")
    .schema(customers_schema)
    .json(f"{DATA_RAW_PATH}/customers.json")
)

# Adiciona metadados de ingestão
df_customers_bronze = (
    df_customers_raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.lit("customers.json"))
)

# Persiste como tabela Delta
df_customers_bronze.write.format("delta").mode("overwrite").saveAsTable(BRONZE_CUSTOMERS)

print(f"✅ {BRONZE_CUSTOMERS}: {df_customers_bronze.count()} registros ingeridos")
df_customers_bronze.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Ingestão: Produtos

# COMMAND ----------

# Schema dos produtos
products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("created_date", StringType(), True)
])

# Leitura do JSON
df_products_raw = (
    spark.read
    .option("multiline", "true")
    .schema(products_schema)
    .json(f"{DATA_RAW_PATH}/products.json")
)

# Adiciona metadados de ingestão
df_products_bronze = (
    df_products_raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.lit("products.json"))
)

# Persiste como tabela Delta
df_products_bronze.write.format("delta").mode("overwrite").saveAsTable(BRONZE_PRODUCTS)

print(f"✅ {BRONZE_PRODUCTS}: {df_products_bronze.count()} registros ingeridos")
df_products_bronze.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Ingestão: Pedidos

# COMMAND ----------

# Schema dos pedidos (com array de itens aninhado)
order_items_schema = ArrayType(StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("item_total", DoubleType(), True)
]))

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("items", order_items_schema, True),
    StructField("total_amount", DoubleType(), True)
])

# Leitura do JSON
df_orders_raw = (
    spark.read
    .option("multiline", "true")
    .schema(orders_schema)
    .json(f"{DATA_RAW_PATH}/orders.json")
)

# Adiciona metadados de ingestão
df_orders_bronze = (
    df_orders_raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.lit("orders.json"))
)

# Persiste como tabela Delta
df_orders_bronze.write.format("delta").mode("overwrite").saveAsTable(BRONZE_ORDERS)

print(f"✅ {BRONZE_ORDERS}: {df_orders_bronze.count()} registros ingeridos")
df_orders_bronze.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Validações da Camada Bronze

# COMMAND ----------

print("=" * 60)
print("  📋 VALIDAÇÕES DA CAMADA BRONZE")
print("=" * 60)

# Verifica contagem de registros
tables = {
    BRONZE_CUSTOMERS: 50,
    BRONZE_PRODUCTS: 30,
    BRONZE_ORDERS: 500
}

all_valid = True
for table_name, expected in tables.items():
    df = spark.table(table_name)
    count = df.count()
    status = "✅" if count >= expected else "❌"
    if count < expected:
        all_valid = False
    print(f"  {status} {table_name}: {count} registros (esperado: {expected})")

print()
if all_valid:
    print("  🎉 TODAS AS VALIDAÇÕES PASSARAM!")
else:
    print("  ⚠️ ALGUMAS VALIDAÇÕES FALHARAM. Verifique os dados de entrada.")

print("=" * 60)
