
# COMMAND ----------

from pyspark.sql import functions as F  # type: ignore
from pyspark.sql.window import Window  # type: ignore

# Hack para parar de dar erro no IDE local (já que spark é injetado apenas no Databricks)
if "spark" not in globals():
    from pyspark.sql import SparkSession # type: ignore
    spark = SparkSession.builder.appName("Local Development").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configurações

# COMMAND ----------

CATALOG_NAME = "workspace"
SILVER_SCHEMA = "silver_ecommerce"
GOLD_SCHEMA = "gold_ecommerce"

# Tabelas Silver (entrada)
SILVER_CUSTOMERS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.customers"
SILVER_PRODUCTS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.products"
SILVER_ORDERS = f"{CATALOG_NAME}.{SILVER_SCHEMA}.orders"

# Tabelas Gold (saída)
GOLD_REVENUE_SUMMARY = f"{CATALOG_NAME}.{GOLD_SCHEMA}.revenue_summary"
GOLD_TOP_CUSTOMERS = f"{CATALOG_NAME}.{GOLD_SCHEMA}.top_customers"
GOLD_TOP_PRODUCTS = f"{CATALOG_NAME}.{GOLD_SCHEMA}.top_products"
GOLD_KPIS = f"{CATALOG_NAME}.{GOLD_SCHEMA}.kpis"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{GOLD_SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Carregamento das Tabelas Silver

# COMMAND ----------

# Carrega tabelas Silver
df_customers = spark.table(SILVER_CUSTOMERS)
df_products = spark.table(SILVER_PRODUCTS)
df_orders = spark.table(SILVER_ORDERS)

# Filtra apenas pedidos completados para métricas de receita
df_orders_completed = df_orders.filter(F.col("status") == "completed")

print(f"📥 Silver Customers: {df_customers.count()} registros")
print(f"📥 Silver Products: {df_products.count()} registros")
print(f"📥 Silver Orders: {df_orders.count()} registros")
print(f"📥 Orders Completed: {df_orders_completed.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💰 Gold: Receita por Período (Mensal)

# COMMAND ----------

# Receita total e por período (mensal)
df_revenue_monthly = (
    df_orders_completed
    .groupBy("order_year_month")
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.count("order_id").alias("total_orders"),
        F.avg("total_amount").alias("avg_ticket"),
        F.min("total_amount").alias("min_order_value"),
        F.max("total_amount").alias("max_order_value"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy("order_year_month")
)

# Calcula crescimento mensal (%)
window_lag = Window.orderBy("order_year_month")
df_revenue_summary = (
    df_revenue_monthly
    .withColumn("prev_revenue", F.lag("total_revenue").over(window_lag))
    .withColumn(
        "growth_pct",
        F.when(
            F.col("prev_revenue").isNotNull() & (F.col("prev_revenue") > 0),
            F.round(
                ((F.col("total_revenue") - F.col("prev_revenue")) / F.col("prev_revenue")) * 100, 
                2
            )
        ).otherwise(F.lit(0.0))
    )
    .withColumn("avg_ticket", F.round(F.col("avg_ticket"), 2))
    .withColumn("total_revenue", F.round(F.col("total_revenue"), 2))
    .withColumn("min_order_value", F.round(F.col("min_order_value"), 2))
    .withColumn("max_order_value", F.round(F.col("max_order_value"), 2))
    .drop("prev_revenue")
    .withColumn("_processed_timestamp", F.current_timestamp())
)

# Salva tabela Gold
df_revenue_summary.write.format("delta").mode("overwrite").saveAsTable(GOLD_REVENUE_SUMMARY)

print(f"✅ {GOLD_REVENUE_SUMMARY}: {df_revenue_summary.count()} registros")
df_revenue_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 👥 Gold: Top Clientes

# COMMAND ----------

# Top clientes por receita
df_top_customers = (
    df_orders_completed
    .groupBy("customer_id")
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.count("order_id").alias("total_orders"),
        F.avg("total_amount").alias("avg_ticket"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date")
    )
    # Join com dados de cliente para enriquecer
    .join(
        df_customers.select("customer_id", "customer_name", "city", "state"),
        on="customer_id",
        how="left"
    )
    # Arredonda valores
    .withColumn("total_revenue", F.round(F.col("total_revenue"), 2))
    .withColumn("avg_ticket", F.round(F.col("avg_ticket"), 2))
    # Ranking
    .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"))))
    .orderBy("rank")
    .withColumn("_processed_timestamp", F.current_timestamp())
)

# Salva tabela Gold
df_top_customers.write.format("delta").mode("overwrite").saveAsTable(GOLD_TOP_CUSTOMERS)

print(f"✅ {GOLD_TOP_CUSTOMERS}: {df_top_customers.count()} registros")
print("\n🏆 Top 10 Clientes por Receita:")
df_top_customers.filter(F.col("rank") <= 10).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Gold: Top Produtos

# COMMAND ----------

# Explode itens dos pedidos completados para análise por produto
df_order_items = (
    df_orders_completed
    .select(
        F.col("order_id"),
        F.col("order_date"),
        F.explode(F.col("items")).alias("item")
    )
    .select(
        "order_id",
        "order_date",
        F.col("item.product_id").alias("product_id"),
        F.col("item.product_name").alias("product_name"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.unit_price").alias("unit_price"),
        F.col("item.item_total").alias("item_total")
    )
)

# Top produtos por quantidade e receita
df_top_products = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("item_total").alias("total_revenue"),
        F.count("order_id").alias("num_orders"),
        F.avg("unit_price").alias("avg_unit_price")
    )
    # Join com dados de produto para enriquecer
    .join(
        df_products.select("product_id", "product_name", "category"),
        on="product_id",
        how="left"
    )
    # Arredonda valores
    .withColumn("total_revenue", F.round(F.col("total_revenue"), 2))
    .withColumn("avg_unit_price", F.round(F.col("avg_unit_price"), 2))
    # Ranking por receita
    .withColumn("rank_revenue", F.row_number().over(Window.orderBy(F.desc("total_revenue"))))
    .withColumn("rank_quantity", F.row_number().over(Window.orderBy(F.desc("total_quantity"))))
    .orderBy("rank_revenue")
    .withColumn("_processed_timestamp", F.current_timestamp())
)

# Salva tabela Gold
df_top_products.write.format("delta").mode("overwrite").saveAsTable(GOLD_TOP_PRODUCTS)

print(f"✅ {GOLD_TOP_PRODUCTS}: {df_top_products.count()} registros")
print("\n🏆 Top 10 Produtos por Receita:")
df_top_products.filter(F.col("rank_revenue") <= 10).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Gold: KPIs Consolidados

# COMMAND ----------

# KPIs gerais do negócio
total_revenue = df_orders_completed.agg(F.sum("total_amount")).collect()[0][0]
total_orders = df_orders_completed.count()
total_customers = df_orders_completed.select("customer_id").distinct().count()
total_products_sold = df_order_items.agg(F.sum("quantity")).collect()[0][0]
avg_ticket = total_revenue / total_orders if total_orders > 0 else 0

# KPIs por método de pagamento
df_kpi_payment = (
    df_orders_completed
    .groupBy("payment_method")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_ticket")
    )
    .withColumn("total_revenue", F.round(F.col("total_revenue"), 2))
    .withColumn("avg_ticket", F.round(F.col("avg_ticket"), 2))
    .withColumn("kpi_type", F.lit("payment_method"))
    .withColumnRenamed("payment_method", "dimension_value")
)

# KPIs por status de pedido (todos os pedidos)
df_kpi_status = (
    df_orders
    .groupBy("status")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_ticket")
    )
    .withColumn("total_revenue", F.round(F.col("total_revenue"), 2))
    .withColumn("avg_ticket", F.round(F.col("avg_ticket"), 2))
    .withColumn("kpi_type", F.lit("order_status"))
    .withColumnRenamed("status", "dimension_value")
)

# KPIs por categoria de produto
df_kpi_category = (
    df_order_items
    .join(df_products.select("product_id", "category"), on="product_id", how="left")
    .groupBy("category")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("item_total").alias("total_revenue"),
        F.avg("item_total").alias("avg_ticket")
    )
    .withColumn("total_revenue", F.round(F.col("total_revenue"), 2))
    .withColumn("avg_ticket", F.round(F.col("avg_ticket"), 2))
    .withColumn("kpi_type", F.lit("product_category"))
    .withColumnRenamed("category", "dimension_value")
)

# Consolida todos os KPIs
df_gold_kpis = (
    df_kpi_payment
    .unionByName(df_kpi_status)
    .unionByName(df_kpi_category)
    .withColumn("_processed_timestamp", F.current_timestamp())
)

# Salva tabela Gold
df_gold_kpis.write.format("delta").mode("overwrite").saveAsTable(GOLD_KPIS)

print(f"✅ {GOLD_KPIS}: {df_gold_kpis.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Resumo dos KPIs

# COMMAND ----------

print("=" * 60)
print("  📊 RESUMO DOS KPIs DO E-COMMERCE")
print("=" * 60)
print(f"""
  💰 Receita Total:              R$ {total_revenue:,.2f}
  🛒 Total de Pedidos:           {total_orders:,}
  👥 Clientes Ativos:            {total_customers:,}
  📦 Produtos Vendidos (unid.):  {total_products_sold:,}
  🎫 Ticket Médio:               R$ {avg_ticket:,.2f}
""")

print("  📊 KPIs por Método de Pagamento:")
df_kpi_payment.show(truncate=False)

print("  📊 KPIs por Status do Pedido:")
df_kpi_status.show(truncate=False)

print("  📊 KPIs por Categoria de Produto:")
df_kpi_category.show(truncate=False)

print("=" * 60)
print("  🎉 CAMADA GOLD CONCLUÍDA COM SUCESSO!")
print("=" * 60)
