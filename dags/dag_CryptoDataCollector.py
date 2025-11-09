import os
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
import redshift_connector
from task.extract import CryptoDataCollectorExtractor
from task.transform import CryptoDataCollectorTransformer
from task.load import CryptoDataCollectorLoader



# Ruta absoluta del directorio del DAG actual
DAG_PATH = os.path.dirname(os.path.realpath(__file__))


# Ruta a la carpeta que está al mismo nivel que dags/
DATA_PATH = os.path.abspath(os.path.join(DAG_PATH, "..", "data"))


with DAG(
    'dag_CryptoDataCollector',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='Extraer, transformar y cargar datos en Amazon Redshift',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:



    extract_task = CryptoDataCollectorExtractor(
        task_id='extract_crypto_data',
        vs_currency="usd",
        total=100,
        per_page=25,
        delay=65, 
        numb_reqs_until_waiting=4,
        output_path = DATA_PATH)

  
    transform_task = CryptoDataCollectorTransformer(
        task_id='transform_crypto_data',
        output_path = DATA_PATH
    )

    load_task = CryptoDataCollectorLoader(
        task_id='load_crypto_data_to_redshift'
    )

 
    # Dependencia de tareas
    extract_task >> transform_task  >> load_task 