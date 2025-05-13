# Libbraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum
import logging
import os
import sys
from dotenv import load_dotenv

# Allows importing modules from the pipeline directory
# This is necessary because the pipeline directory is not in the Python path by default
sys.path.append('/opt/airflow')
from pipeline.extract import get_data_from_api
from pipeline.transform import transform_and_clean
from pipeline.load import load_to_gold

# Import functions from the validation module
from pipeline.validate import validate_bronze_data, validate_silver_data, validate_gold_data

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/opt/airflow/logs/brewery_etl_dag.log"),
    ],
)

# Load environment variables
load_dotenv('/opt/.env')
BRONZE_DIR = os.getenv('BRONZE_DIR')
SILVER_DIR = os.getenv('SILVER_DIR')
GOLD_DIR = os.getenv('GOLD_DIR')

for var_dir in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    if not var_dir:
        # Checks if the environment variables are set
        # If not, logs the error and raises an exception
        message_var = (
            "As variáveis de ambiente BRONZE_DIR, SILVER_DIR e GOLD_DIR devem ser definidas no .env"
        )
        logging.error(f"{var_dir}")
        raise EnvironmentError(f"{var_dir}")

# Simulates sending an email alert
# In a real scenario, we would use an email service to send the alert
def email_alert(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    logging.warning(
        f"[ALERTA DE E-MAIL] DAG '{dag_id}' - Task '{task_id}' falhou em {execution_date}. "
        f"Logs: {log_url}"
    )

# DAG 
with DAG(
    dag_id="brewery_etl_dag",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 5, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": email_alert,
    },
    tags=["brewery", "etl"],
) as dag:
    
    # Extract data from API and save to bronze directory
    extract_task = PythonOperator(
        task_id="get_data_from_api",
        python_callable=get_data_from_api,
        op_args=["https://api.openbrewerydb.org/v1/breweries", BRONZE_DIR],
    )
    
    # Validação Bronze
    validate_bronze = PythonOperator(
        task_id="validate_bronze_data",
        python_callable=validate_bronze_data,
        op_args=[BRONZE_DIR],
    )


    # Transform and clean data from bronze directory to silver directory
    transform = PythonOperator(
        task_id="transform_and_clean",
        python_callable=transform_and_clean,
        op_kwargs={
            "bronze_path": BRONZE_DIR,
            "silver_path": SILVER_DIR,
        },
    )
    
    # Validação Silver
    validate_silver = PythonOperator(
        task_id="validate_silver_data",
        python_callable=validate_silver_data,
        op_args=[SILVER_DIR],
    )


    # Load data from silver directory to gold directory
    load = PythonOperator(
        task_id="load_to_gold",
        python_callable=load_to_gold,
        op_kwargs={
            "silver_path": SILVER_DIR,
            "gold_path": GOLD_DIR,
        },
    )

    # Validação Gold
    validate_gold = PythonOperator(
        task_id="validate_gold_data",
        python_callable=validate_gold_data,
        op_args=[GOLD_DIR],
    )    

    # Define tasks order
    extract_task >> validate_bronze >> transform >> validate_silver >> load >> validate_gold
    