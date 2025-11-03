import os
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from task.extract import CryptoDataCollectorExtractor


# Constantes
REDSHIFT_CONN_STRING = f""
REDSHIFT_TABLE = 'example_table'

# Ruta absoluta del directorio del DAG actual
DAG_PATH = os.path.dirname(os.path.realpath(__file__))



# Ruta a la carpeta que está al mismo nivel que dags/
DATA_PATH = os.path.abspath(os.path.join(DAG_PATH, "..", "data"))


def extract_data(**kwargs):
    output_parquet = kwargs['output_parquet']
    extractor = CryptoDataCollectorExtractor(vs_currency="usd", total=500)
    extractor.get_all_pages()
    df = extractor.get_dataframe()
    print("Contenido del DataFrame:")
    print(df.head(10).to_string(index=False))
    path = os.path.join(output_parquet, 'extracted_data.parquet')
    df.to_parquet(path)
    if df.empty:
        raise AirflowSkipException('No se extrayeron datos')
    return path


def transform_data(**kwargs):
    input_parquet = kwargs['ti'].xcom_pull(task_ids='extract_data')
    output_parquet = kwargs['output_parquet']
    df = pd.read_parquet(input_parquet)
    df_transformed = df
    path = os.path.join(output_parquet, 'transformed_data.parquet')
    df_transformed.to_parquet(path, index=False)
    return path


# Define DAG
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


    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={'output_parquet': DATA_PATH},
    )

  
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={'output_parquet': DATA_PATH},
    )

 
    # Dependencia de tareas
    extract_task >> transform_task 