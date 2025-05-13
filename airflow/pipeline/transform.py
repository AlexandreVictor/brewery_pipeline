from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when
from pyspark.sql.types import DoubleType
import os
import logging
import shutil

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def transform_and_clean(bronze_path: str, silver_path: str) -> str:
    """
    Transforma os dados da camada Bronze para a Silver:
    - Seleciona campos relevantes
    - Padroniza tipos e valores
    - Remove nulos críticos
    - Salva em formato Parquet particionado por país

    Args:
        bronze_path (str): Caminho dos arquivos Parquet da Bronze
        silver_path (str): Caminho de destino da Silver

    Returns:
        str: Caminho final onde os dados Silver foram salvos
    """
    print(f"[DEBUG] silver_path = {silver_path}")
    try:
        logger.info("[silver] Iniciando transformação Bronze → Silver")

        spark = SparkSession.builder.appName("SilverTransform").getOrCreate()


        if not os.path.exists(silver_path):
            os.makedirs(silver_path)

        # Find the latest file in the Bronze directory.
        files = [f for f in os.listdir(bronze_path) if f.endswith(".parquet")]
        if not files:
            raise FileNotFoundError("Nenhum arquivo Parquet encontrado em Bronze")

        latest = sorted(files)[-1]
        full_path = os.path.join(bronze_path, latest)
        logger.info(f"[silver] Lendo arquivo: {full_path}")

        df = spark.read.parquet(full_path)

        # Data cleaned and transformed
        df_clean = (
            df
            .select(
                col("id").alias("brewery_id"),
                trim(col("name")).alias("name"),
                lower(trim(col("brewery_type"))).alias("type"),
                trim(col("city")).alias("city"),
                trim(col("state")).alias("state"),
                trim(col("state_province")).alias("state_province"),
                trim(col("country")).alias("country"),
                col("latitude").cast(DoubleType()),
                col("longitude").cast(DoubleType()),
                trim(col("phone")).alias("phone"),
                trim(col("website_url")).alias("website"),
                trim(col("street")).alias("address")
            )
            .withColumn("type", when(col("type").isNull(), "unknown").otherwise(col("type")))
            .dropna(subset=["brewery_id", "name", "country"])
        )

        logger.info(f"[silver] Registros após limpeza: {df_clean.count()}")
        # Save the cleaned DataFrame to the Silver directory
        df_clean.write.mode("overwrite").partitionBy("country").parquet(silver_path)
        logger.info(f"[silver] Dados salvos com sucesso em: {silver_path}")

        return silver_path

    except Exception as e:
        logger.error(f"[silver] Erro na transformação: {e}", exc_info=True)
        raise
