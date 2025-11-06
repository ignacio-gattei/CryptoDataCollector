import os
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
import redshift_connector
from task.extract import CryptoDataCollectorExtractor


# --- Datos de conexión ---
host = "redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com"  # ejemplo: redshift-cluster-1.abc123xyz789.us-east-1.redshift.amazonaws.com
database = "pda"
user = "2025_ignacio_gattei"
password = "1ERK9lnO17Xrh"
port = 5439


REDSHIFT_TABLE = 'tabla_prueba'

# Ruta absoluta del directorio del DAG actual
DAG_PATH = os.path.dirname(os.path.realpath(__file__))


# Ruta a la carpeta que está al mismo nivel que dags/
DATA_PATH = os.path.abspath(os.path.join(DAG_PATH, "..", "data"))


def extract_data(**kwargs):
    output_parquet = kwargs['output_parquet']
    extractor = CryptoDataCollectorExtractor(vs_currency="usd", total=250)
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


def load_to_redshift(**kwargs):
    transformed_parquet = kwargs['ti'].xcom_pull(task_ids='transform_data')
    redshift_table = kwargs['redshift_table']
    df = pd.read_parquet(transformed_parquet)

    try:
        conn = redshift_connector.connect(
        host=os.environ['REDSHIFT_HOST'],
        database=os.environ['REDSHIFT_DATABASE'],
        user=os.environ['REDSHIFT_USER'],
        password=os.environ['REDSHIFT_PASSWORD'],
        port=int(os.getenv("REDSHIFT_PORT", 5439)))
    except Exception as e:
        raise AirflowSkipException('Error de conexion a la DB')
    
    cursor = conn.cursor()

    cursor.execute("""SET search_path TO "2025_ignacio_gattei_schema";""")
    conn.commit()


    cursor.execute("DROP TABLE IF EXISTS CRYPTOCURRENCIES;")
    conn.commit()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS CRYPTOCURRENCIES (
    id VARCHAR(100),
    symbol VARCHAR(100),
    name VARCHAR(500),
    image VARCHAR(1000)
    )
    """)
    conn.commit()

 

    data = [
    (str(row.id), str(row.symbol), str(row.name), str(row.image))
    for row in df.itertuples(index=False)
    ]

    cursor.executemany(
        "INSERT INTO CRIPTOMONEDAS (id, symbol, name, image) VALUES (%s, %s, %s, %s)",
     data
    )

    conn.commit()


    cursor.close()
    conn.close()  


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

    load_task = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
        op_kwargs={
            'redshift_table': REDSHIFT_TABLE
        },
    )

 
    # Dependencia de tareas
    extract_task >> transform_task  >> load_task 