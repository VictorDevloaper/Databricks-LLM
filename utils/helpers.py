# ============================================================================
# Funções Utilitárias - Data Pipeline + RAG com Databricks
# ============================================================================
# Funções reutilizáveis para leitura/escrita Delta, validação,
# logging e formatação de dados.
# ============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from datetime import datetime
import logging


# ----------------------------------------------------------------------------
# LOGGING
# ----------------------------------------------------------------------------
def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Cria e retorna um logger configurado."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# ----------------------------------------------------------------------------
# SPARK SESSION
# ----------------------------------------------------------------------------
def get_spark() -> SparkSession:
    """Retorna a SparkSession ativa (no Databricks, já existe por padrão)."""
    return SparkSession.builder.getOrCreate()


# ----------------------------------------------------------------------------
# LEITURA E ESCRITA DELTA
# ----------------------------------------------------------------------------
def read_delta_table(table_name: str) -> DataFrame:
    """Lê uma tabela Delta pelo nome completo (catalog.schema.table)."""
    spark = get_spark()
    logger = get_logger("helpers")
    logger.info(f"Lendo tabela Delta: {table_name}")
    return spark.table(table_name)


def write_delta_table(
    df: DataFrame, 
    table_name: str, 
    mode: str = "overwrite",
    partition_by: list = None
) -> None:
    """
    Escreve um DataFrame como tabela Delta.

    Args:
        df: DataFrame a ser salvo
        table_name: Nome completo da tabela (catalog.schema.table)
        mode: Modo de escrita ('overwrite', 'append')
        partition_by: Lista de colunas para particionamento (opcional)
    """
    logger = get_logger("helpers")
    logger.info(f"Escrevendo tabela Delta: {table_name} (mode={mode})")

    writer = df.write.format("delta").mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.saveAsTable(table_name)
    
    count = df.count()
    logger.info(f"✅ Tabela {table_name} salva com {count} registros")


def read_json_to_df(file_path: str, schema: StructType = None) -> DataFrame:
    """
    Lê um arquivo JSON e retorna um DataFrame.

    Args:
        file_path: Caminho do arquivo JSON
        schema: Schema opcional para forçar tipos
    """
    spark = get_spark()
    logger = get_logger("helpers")
    logger.info(f"Lendo arquivo JSON: {file_path}")

    reader = spark.read.option("multiline", "true")

    if schema:
        reader = reader.schema(schema)

    df = reader.json(file_path)
    logger.info(f"✅ JSON lido com {df.count()} registros")
    return df


# ----------------------------------------------------------------------------
# VALIDAÇÕES DE DADOS
# ----------------------------------------------------------------------------
def validate_no_duplicates(df: DataFrame, key_columns: list, table_name: str) -> bool:
    """
    Valida que não existem duplicatas com base nas colunas-chave.

    Args:
        df: DataFrame a validar
        key_columns: Lista de colunas que formam a chave única
        table_name: Nome da tabela (para logging)

    Returns:
        True se não há duplicatas, False caso contrário
    """
    logger = get_logger("validation")

    total = df.count()
    distinct = df.dropDuplicates(key_columns).count()
    duplicates = total - distinct

    if duplicates > 0:
        logger.warning(f"⚠️ {table_name}: {duplicates} duplicatas encontradas nas colunas {key_columns}")
        return False
    else:
        logger.info(f"✅ {table_name}: Nenhuma duplicata encontrada ({total} registros)")
        return True


def validate_no_nulls(df: DataFrame, columns: list, table_name: str) -> bool:
    """
    Valida que não existem valores nulos nas colunas especificadas.

    Args:
        df: DataFrame a validar
        columns: Lista de colunas que não devem ter nulos
        table_name: Nome da tabela (para logging)

    Returns:
        True se não há nulos, False caso contrário
    """
    logger = get_logger("validation")
    all_valid = True

    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            logger.warning(f"⚠️ {table_name}.{col}: {null_count} valores nulos")
            all_valid = False
        else:
            logger.info(f"✅ {table_name}.{col}: Sem valores nulos")

    return all_valid


def validate_record_count(df: DataFrame, table_name: str, expected_min: int = 1) -> bool:
    """Valida que o DataFrame tem pelo menos o número mínimo de registros."""
    logger = get_logger("validation")
    count = df.count()

    if count >= expected_min:
        logger.info(f"✅ {table_name}: {count} registros (mínimo esperado: {expected_min})")
        return True
    else:
        logger.error(f"❌ {table_name}: {count} registros (esperado mínimo: {expected_min})")
        return False


# ----------------------------------------------------------------------------
# TRANSFORMAÇÕES COMUNS
# ----------------------------------------------------------------------------
def add_ingestion_timestamp(df: DataFrame) -> DataFrame:
    """Adiciona coluna _ingestion_timestamp com o timestamp atual."""
    return df.withColumn("_ingestion_timestamp", F.current_timestamp())


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Padroniza nomes de colunas para snake_case e minúsculas.
    Remove espaços e caracteres especiais.
    """
    import re
    new_columns = []
    for col_name in df.columns:
        # Converte CamelCase para snake_case
        new_name = re.sub(r'(?<!^)(?=[A-Z])', '_', col_name).lower()
        # Remove caracteres especiais
        new_name = re.sub(r'[^a-z0-9_]', '_', new_name)
        # Remove underscores duplicados
        new_name = re.sub(r'_+', '_', new_name).strip('_')
        new_columns.append(new_name)

    for old, new in zip(df.columns, new_columns):
        if old != new:
            df = df.withColumnRenamed(old, new)

    return df


def format_currency(value: float) -> str:
    """Formata um valor numérico como moeda brasileira."""
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ----------------------------------------------------------------------------
# FORMATAÇÃO PARA EMBEDDINGS
# ----------------------------------------------------------------------------
def format_customer_text(row) -> str:
    """Formata um registro de cliente como texto descritivo para embedding."""
    return (
        f"Cliente {row['customer_name']} da cidade {row['city']}, "
        f"estado {row['state']}. "
        f"Total de pedidos: {row['total_orders']}. "
        f"Receita total gerada: {format_currency(row['total_revenue'])}. "
        f"Ticket médio: {format_currency(row['avg_ticket'])}."
    )


def format_product_text(row) -> str:
    """Formata um registro de produto como texto descritivo para embedding."""
    return (
        f"Produto '{row['product_name']}' da categoria '{row['category']}'. "
        f"Preço unitário: {format_currency(row['price'])}. "
        f"Quantidade total vendida: {row['total_quantity']}. "
        f"Receita total gerada: {format_currency(row['total_revenue'])}."
    )


def format_kpi_text(row) -> str:
    """Formata um registro de KPI como texto descritivo para embedding."""
    return (
        f"No período {row['period']}: "
        f"Receita total de {format_currency(row['total_revenue'])}. "
        f"Total de {row['total_orders']} pedidos realizados. "
        f"Ticket médio de {format_currency(row['avg_ticket'])}. "
        f"Crescimento em relação ao período anterior: {row['growth_pct']:.1f}%."
    )


# ----------------------------------------------------------------------------
# UTILITÁRIOS GERAIS
# ----------------------------------------------------------------------------
def print_separator(title: str = "") -> None:
    """Imprime um separador visual com título opcional."""
    line = "=" * 60
    if title:
        print(f"\n{line}")
        print(f"  {title}")
        print(f"{line}")
    else:
        print(f"\n{line}")


def print_dataframe_info(df: DataFrame, table_name: str) -> None:
    """Imprime informações resumidas de um DataFrame."""
    print_separator(f"📊 Informações: {table_name}")
    print(f"  Registros: {df.count()}")
    print(f"  Colunas: {len(df.columns)}")
    print(f"  Schema:")
    df.printSchema()
    print(f"  Amostra (5 primeiros):")
    df.show(5, truncate=False)
