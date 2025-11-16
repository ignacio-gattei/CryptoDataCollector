import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

from dags.task.extract_crypto_data import CryptoDataCollectorExtractor
from dags.task.transform_crypto_data import CryptoDataCollectorTransformer
from dags.task.load_crypto_data import CryptoDataCollectorLoader
from task.extract_exchange_rate import ExchangeRateExtractor
from task.transform_exchange_rate import ExchangeRateTransformer
from task.load_exchange_rate import ExchangeRateLoader
from task.generate_summary_data import SummaryGenerator


# Ruta absoluta del directorio del DAG actual
DAG_PATH = os.path.dirname(os.path.realpath(__file__))

# Ruta a la carpeta donde se guardarán los datos extraídos y transformados
DATA_PATH = os.path.abspath(os.path.join(DAG_PATH, "..", "data"))

ENDPOINT_API_CRYPTO = "https://api.coingecko.com/api/v3/coins/markets"
ENDPOINT_API_EXCHANGE_RATE = "https://dolarapi.com/v1/dolares/bolsa"

with DAG(
    "dag_CryptoDataCollector",
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    },
    description="Extraer, transformar y cargar cotizaciones de cryptos en Amazon Redshift",
    schedule_interval="*/30 * * * *",  # Ejecuta cada 30 minutos
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract_crypto_data = CryptoDataCollectorExtractor(
        task_id="extract_crypto_data",
        total_extract=10,
        api_endpoint=ENDPOINT_API_CRYPTO,
        output_path=DATA_PATH,
        output_file_name="{{ ts_nodash }}_extracted_crypto_data.parquet",
    )

    transform_crypto_data = CryptoDataCollectorTransformer(
        task_id="transform_crypto_data",
        output_path=DATA_PATH,
        output_file_name="{{ ts_nodash }}_transformed_crypto_data.parquet",
    )

    load_crypto_data = CryptoDataCollectorLoader(
        task_id="load_crypto_data_to_redshift"
    )

    extract_exchange_rate = ExchangeRateExtractor(
        task_id="extract_exchange_rate",
        api_endpoint=ENDPOINT_API_EXCHANGE_RATE,
        source_currency="USD",
        target_currency="ARS",
        output_path=DATA_PATH,
        output_file_name="{{ ts_nodash }}_extracted_exchange_rate.parquet",
    )

    transform_exchange_rate = ExchangeRateTransformer(
        task_id="transform_exchange_rate",
        output_path=DATA_PATH,
        output_file_name="{{ ts_nodash }}_transformed_exchange_rate.parquet",
    )

    load_exchange_rate = ExchangeRateLoader(
        task_id="load_exchange_rate_to_redshift"
    )

    generate_summary_data = SummaryGenerator(
        task_id="generate_summary_data"
    )

    # Dependencia de tareas
    extract_exchange_rate >> transform_exchange_rate >> load_exchange_rate
    extract_crypto_data >> transform_crypto_data >> load_crypto_data
    load_exchange_rate >> load_crypto_data >> generate_summary_data
