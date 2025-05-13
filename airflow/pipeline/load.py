from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count,  asc
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_to_gold(silver_path: str, gold_path: str) -> str:
    """
    Realiza agregação analítica sobre a camada Silver e salva o resultado na Gold.

    Args:
        silver_path (str): Caminho da Silver Layer particionada
        gold_path (str): Caminho da Gold Layer onde salvar o resultado

    Returns:
        str: Caminho onde o arquivo Parquet foi salvo
    """
    try:
        logger.info("[gold] Iniciando agregação Silver → Gold")

        spark = SparkSession.builder.appName("GoldAggregation").getOrCreate()
        df = spark.read.parquet(silver_path)

        logger.info(f"[gold] Registros lidos da Silver: {df.count()}")
        # Aggregation by country, state, and type
        # Count the number of breweries in each group
        df_agg = (
            df.groupBy("country", "state", "type")
              .agg(count("*").alias("brewery_count"))
              .orderBy("country", "state", "brewery_count", ascending=False)
        )

        logger.info(f"[gold] Total de grupos agregados: {df_agg.count()}")
        
        #show top 10 and order by brewery_count
        logger.info("[gold] Exibindo os 10 primeiros grupos agregados:")
        df_agg.sort(asc("brewery_count")).show(10)
    
        df_agg.write.mode("overwrite").parquet(gold_path)
        logger.info(f"[gold] Dados agregados salvos em: {gold_path}")

        return gold_path

    except Exception as e:
        logger.error(f"[gold] Erro na agregação: {e}", exc_info=True)
        raise
