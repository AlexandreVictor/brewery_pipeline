import requests
import os
import pandas as pd
import logging
import time
from datetime import datetime
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def get_data_from_api(api_url: str, bronze_path: str, per_page: int = 200, max_retries: int = 3) -> str:
    """
    Extrai todos os dados da Open Brewery API (com paginação), com tratamento de erros e salvamento em Parquet.

    Args:
        api_url (str): URL base da API
        bronze_path (str): Diretório onde os arquivos serão salvos
        per_page (int): Quantos registros por página
        max_retries (int): Quantas tentativas em caso de falha de rede

    Returns:
        str: Caminho do arquivo Parquet salvo
    """
    try:
        print(f"[DEBUG] bronze_path = {bronze_path}")
        os.makedirs(bronze_path, exist_ok=True)

        logger.info(f"[extract] Início da extração: {datetime.now().isoformat()}")
        all_breweries = []
        page = 1

        while True:
            paginated_url = f"{api_url}?page={page}&per_page={per_page}"
            logger.info(f"[extract] Requisição página {page}: {paginated_url}")

            retries = 0
            while retries < max_retries:
                try:
                    response = requests.get(paginated_url, timeout=10)
                    response.raise_for_status()
                    breweries = response.json()
                    break  # success
                except (Timeout, ConnectionError) as net_err:
                    logger.warning(f"[extract] Erro de rede (página {page}): {net_err}. Tentando novamente...")
                    retries += 1
                    time.sleep(2 ** retries)  # backoff exponencial
                except HTTPError as http_err:
                    logger.error(f"[extract] Erro HTTP (página {page}): {http_err}")
                    raise #failure
                except RequestException as req_err:
                    logger.error(f"[extract] Erro inesperado na request (página {page}): {req_err}")
                    raise #failure

            if retries == max_retries:
                logger.error(f"[extract] Falha após {max_retries} tentativas na página {page}. Encerrando...")
                raise ConnectionError(f"Falha na página {page} após várias tentativas")

            if not breweries:
                logger.info("[extract] Nenhum dado retornado. Fim da extração.")
                break

            all_breweries.extend(breweries)
            logger.info(f"[extract] Página {page} OK: {len(breweries)} registros")
            page += 1

        # Save to Parquet
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        parquet_path = os.path.join(bronze_path, f"breweries_raw_{timestamp}.parquet")
        logger.info(f"Total de registros obtidos: {len(all_breweries)} registros") #8730 rows
        df = pd.DataFrame(all_breweries)
        df.to_parquet(parquet_path, index=False)
        logger.info(f"[extract] Parquet salvo: {parquet_path}")
        return parquet_path

    except Exception as e:
        logger.error(f"[extract] Erro fatal na extração: {e}", exc_info=True)
        raise
