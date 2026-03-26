# Databricks notebook source

# ============================================================================
# ETAPA 2 — CAMADA SILVER (Tratamento e Limpeza)
# ============================================================================
# Objetivo: Transformar dados brutos da Bronze em dados limpos e padronizados.
#
# Entrada: Tabelas Delta Bronze
# Saída:   Tabelas Delta Silver (silver_customers, silver_products, silver_orders)
#
# Atividades:
#   - Remoção de duplicatas
#   - Tratamento de valores nulos
#   - Padronização de colunas e tipos
#   - Normalização de datas e valores monetários
# ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥈 Camada Silver — Tratamento e Limpeza de Dados
# MAGIC
# MAGIC Transforma os dados brutos da camada Bronze em dados limpos,
# MAGIC padronizados e prontos para análise.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configurações

# COMMAND ----------

CATALOG_NAME = "workspace"
BRONZE_SCHEMA = "bronze_ecommerce"
SILVER_SCHEMA = "silver_ecommerce"

# Tabelas Bronze (entrada)
BRONZE_CUSTOMERS = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.customers"
BRONZE_PRODUCTS = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.products"
BRONZE_ORDERS = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.orders"

# Tabelas Silver (saída)
SILVER_CUSTOMERS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.customers"
SILVER_PRODUCTS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.products"
SILVER_ORDERS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.orders"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SILVER_SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 👤 Tratamento: Clientes

# COMMAND ----------

# Leitura da Bronze
df_customers_bronze = spark.table(BRONZE_CUSTOMERS)

print(f"📥 Bronze Customers: {df_customers_bronze.count()} registros")

# --- Tratamento ---
df_customers_silver = (
    df_customers_bronze
    # 1. Remove duplicatas por customer_id
    .dropDuplicates(["customer_id"])
    
    # 2. Remove registros sem ID
    .filter(F.col("customer_id").isNotNull())
    
    # 3. Trata nulos em campos de texto
    .fillna({
        "customer_name": "Nome Desconhecido",
        "email": "sem_email@unknown.com",
        "city": "Cidade Desconhecida",
        "state": "XX"
    })
    
    # 4. Padroniza nomes (capitalização)
    .withColumn("customer_name", F.initcap(F.trim(F.col("customer_name"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("state", F.upper(F.trim(F.col("state"))))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    
    # 5. Converte data de registro para tipo Date
    .withColumn("registration_date", F.to_date(F.col("registration_date"), "yyyy-MM-dd"))
    
    # 6. Adiciona timestamp de processamento
    .withColumn("_processed_timestamp", F.current_timestamp())
    
    # 7. Remove colunas de metadados da Bronze
    .drop("_ingestion_timestamp", "_source_file")
)

# Persiste como tabela Delta Silver
df_customers_silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_CUSTOMERS)

print(f"✅ {SILVER_CUSTOMERS}: {df_customers_silver.count()} registros processados")
df_customers_silver.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Tratamento: Produtos

# COMMAND ----------

# Leitura da Bronze
df_products_bronze = spark.table(BRONZE_PRODUCTS)

print(f"📥 Bronze Products: {df_products_bronze.count()} registros")

# --- Tratamento ---
df_products_silver = (
    df_products_bronze
    # 1. Remove duplicatas por product_id
    .dropDuplicates(["product_id"])
    
    # 2. Remove registros sem ID
    .filter(F.col("product_id").isNotNull())
    
    # 3. Trata nulos
    .fillna({
        "product_name": "Produto Sem Nome",
        "category": "Sem Categoria",
        "price": 0.0,
        "stock_quantity": 0
    })
    
    # 4. Padroniza textos
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
    
    # 5. Garante tipos corretos
    .withColumn("price", F.round(F.col("price").cast(DoubleType()), 2))
    .withColumn("stock_quantity", F.col("stock_quantity").cast(IntegerType()))
    
    # 6. Remove preços negativos ou zero
    .withColumn("price", F.when(F.col("price") <= 0, F.lit(0.01)).otherwise(F.col("price")))
    
    # 7. Converte data de criação
    .withColumn("created_date", F.to_date(F.col("created_date"), "yyyy-MM-dd"))
    
    # 8. Adiciona timestamp de processamento
    .withColumn("_processed_timestamp", F.current_timestamp())
    
    # 9. Remove colunas de metadados da Bronze
    .drop("_ingestion_timestamp", "_source_file")
)

# Persiste como tabela Delta Silver
df_products_silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_PRODUCTS)

print(f"✅ {SILVER_PRODUCTS}: {df_products_silver.count()} registros processados")
df_products_silver.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛒 Tratamento: Pedidos

# COMMAND ----------

# Leitura da Bronze
df_orders_bronze = spark.table(BRONZE_ORDERS)

print(f"📥 Bronze Orders: {df_orders_bronze.count()} registros")

# --- Tratamento ---

# Primeiro, explode os itens do pedido para ter uma linha por item
df_orders_exploded = (
    df_orders_bronze
    # 1. Remove duplicatas por order_id
    .dropDuplicates(["order_id"])
    
    # 2. Remove registros sem ID ou sem cliente
    .filter(
        F.col("order_id").isNotNull() & 
        F.col("customer_id").isNotNull()
    )
    
    # 3. Filtra apenas pedidos com status válido
    .filter(F.col("status").isin("completed", "processing", "shipped", "cancelled"))
    
    # 4. Padroniza textos
    .withColumn("status", F.lower(F.trim(F.col("status"))))
    .withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))
    
    # 5. Converte data do pedido
    .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
    
    # 6. Extrai ano e mês para facilitar agregações
    .withColumn("order_year", F.year(F.col("order_date")))
    .withColumn("order_month", F.month(F.col("order_date")))
    .withColumn("order_year_month", F.date_format(F.col("order_date"), "yyyy-MM"))
    
    # 7. Garante tipo correto para total
    .withColumn("total_amount", F.round(F.col("total_amount").cast(DoubleType()), 2))
    
    # 8. Calcula número de itens do pedido
    .withColumn("num_items", F.size(F.col("items")))
    
    # 9. Adiciona timestamp de processamento
    .withColumn("_processed_timestamp", F.current_timestamp())
    
    # 10. Remove colunas de metadados da Bronze
    .drop("_ingestion_timestamp", "_source_file")
)

# Tabela Silver de pedidos (nível pedido — mantém array de itens)
df_orders_silver = df_orders_exploded

# Persiste como tabela Delta Silver
df_orders_silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_ORDERS)

print(f"✅ {SILVER_ORDERS}: {df_orders_silver.count()} registros processados")
df_orders_silver.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Validações da Camada Silver

# COMMAND ----------

print("=" * 60)
print("  📋 VALIDAÇÕES DA CAMADA SILVER")
print("=" * 60)

# --- Validação 1: Sem duplicatas ---
print("\n📌 1. Verificação de Duplicatas:")

for table, key_col in [
    (SILVER_CUSTOMERS, "customer_id"),
    (SILVER_PRODUCTS, "product_id"),
    (SILVER_ORDERS, "order_id")
]:
    df = spark.table(table)
    total = df.count()
    distinct = df.select(key_col).distinct().count()
    status = "✅" if total == distinct else "❌"
    print(f"  {status} {table}: {total} registros, {distinct} únicos")

# --- Validação 2: Sem nulos em campos obrigatórios ---
print("\n📌 2. Verificação de Nulos em Campos Obrigatórios:")

validations = [
    (SILVER_CUSTOMERS, ["customer_id", "customer_name", "email"]),
    (SILVER_PRODUCTS, ["product_id", "product_name", "price"]),
    (SILVER_ORDERS, ["order_id", "customer_id", "order_date", "total_amount"])
]

for table, columns in validations:
    df = spark.table(table)
    for col_name in columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        status = "✅" if null_count == 0 else f"❌ ({null_count} nulos)"
        print(f"  {status} {table}.{col_name}")

# --- Validação 3: Tipos de dados corretos ---
print("\n📌 3. Verificação de Tipos de Dados:")

df_silver_orders = spark.table(SILVER_ORDERS)
print(f"  order_date tipo: {df_silver_orders.schema['order_date'].dataType}")
print(f"  total_amount tipo: {df_silver_orders.schema['total_amount'].dataType}")

df_silver_products = spark.table(SILVER_PRODUCTS)
print(f"  price tipo: {df_silver_products.schema['price'].dataType}")

print("\n" + "=" * 60)
print("  🎉 VALIDAÇÕES DA SILVER CONCLUÍDAS!")
print("=" * 60)
