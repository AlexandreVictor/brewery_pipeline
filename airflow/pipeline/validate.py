import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Bronze — check whether the file exists and has rows
def validate_bronze_data(bronze_path: str):
    logger.info("[validate_bronze] Iniciando validação do Bronze...")
    files = [f for f in os.listdir(bronze_path) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo Parquet encontrado na Bronze Layer.")

    latest = sorted(files)[-1]
    full_path = os.path.join(bronze_path, latest)
    df = pd.read_parquet(full_path)

    if df.empty:
        raise ValueError("Arquivo Parquet da Bronze está vazio.")

    logger.info(f"[validate_bronze] OK: {len(df)} registros em {latest}.")


# Silver — Check schema and critical null values
def validate_silver_data(silver_path: str):
    logger.info("[validate_silver] Iniciando validação da Silver...")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(silver_path)

    total = df.count()
    if total == 0:
        raise ValueError("Silver Layer vazia.")

    missing = df.filter(col("brewery_id").isNull() | col("name").isNull()).count()
    if missing > 0:
        raise ValueError(f"Silver Layer possui {missing} registros com ID ou nome nulos.")

    logger.info(f"[validate_silver] OK: {total} registros válidos.")


# Gold — Check aggregation, whether the file exists, and critical null values
def validate_gold_data(gold_path: str):
    logger.info("[validate_gold] Iniciando validação da Gold...")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(gold_path)

    total = df.count()
    if total == 0:
        raise ValueError("Gold Layer vazia.")

    if df.filter(col("brewery_count").isNull()).count() > 0:
        raise ValueError("Gold possui brewery_count nulo.")

    logger.info(f"[validate_gold] OK: {total} grupos agregados.")
