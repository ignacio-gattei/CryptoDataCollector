"""
Operador Airflow para cargar datos de criptomonedas en Redshift.
Incluye creación de tablas, inserción de datos y borrado de tablas.
"""

import os
import pandas as pd
import redshift_connector
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException
from utils.functions import format_number_short

CREATE_TABLE_STG_CRYPTOCURRENCIES_DATA = """
CREATE TABLE IF NOT EXISTS STG_CRYPTOCURRENCIES_DATA (
    id VARCHAR(100),
    symbol VARCHAR(100),
    name VARCHAR(500),
    image VARCHAR(500),
    current_price_usd DECIMAL(38,18),
    market_cap DECIMAL(38,2),
    market_cap_short_number VARCHAR(50),
    market_cap_rank INT,
    fully_diluted_valuation BIGINT,
    total_volume BIGINT,
    high_24h DECIMAL(38,18),
    low_24h DECIMAL(38,18),
    price_change_24h DECIMAL(38,18),
    price_change_percentage_24h DECIMAL(38,18),
    market_cap_change_24h DECIMAL(38,2),
    market_cap_change_percentage_24h DECIMAL(38,2),
    price_change_since_last_update DECIMAL(38,18),
    price_change_percentage_since_last_update DECIMAL(38,18),
    circulating_supply BIGINT,
    total_supply BIGINT,
    max_supply BIGINT,
    ath DECIMAL(38,18),
    ath_change_percentage DECIMAL(38,18),
    ath_date TIMESTAMP,
    atl DECIMAL(38,18),
    atl_change_percentage DECIMAL(38,18),
    atl_date TIMESTAMP,
    load_date TIMESTAMP DEFAULT GETDATE(),
    last_updated TIMESTAMP,
    dag_run_id VARCHAR(50)
);
"""

CREATE_TABLE_DIM_CRYPTOCURRENCIES = """
CREATE TABLE IF NOT EXISTS DIM_CRYPTOCURRENCIES (
    id VARCHAR(100) PRIMARY KEY,
    symbol VARCHAR(100),
    name VARCHAR(500),
    image VARCHAR(1000)
);
"""

CREATE_TABLE_FACTS_CRYPTOCURRENCIES = """
CREATE TABLE IF NOT EXISTS FACTS_CRYPTOCURRENCIES (
    id VARCHAR(50),
    current_price_usd DECIMAL(38,18),
    current_price_other_currency DECIMAL(38,18),
    other_currency VARCHAR(5),
    market_cap DECIMAL(38,2),
    market_cap_short_number VARCHAR(50),
    market_cap_rank INT,
    fully_diluted_valuation BIGINT,
    total_volume BIGINT,
    high_24h DECIMAL(38,18),
    low_24h DECIMAL(38,18),
    price_change_24h DECIMAL(38,18),
    price_change_percentage_24h DECIMAL(38,18),
    price_change_since_last_update DECIMAL(38,18),
    price_change_percentage_since_last_update DECIMAL(38,18),
    market_cap_change_24h DECIMAL(38,2),
    market_cap_change_percentage_24h DECIMAL(38,2),
    circulating_supply BIGINT,
    total_supply BIGINT,
    max_supply BIGINT,
    ath DECIMAL(38,18),
    ath_change_percentage DECIMAL(38,18),
    ath_date TIMESTAMP,
    atl DECIMAL(38,18),
    atl_change_percentage DECIMAL(38,18),
    atl_date TIMESTAMP,
    load_date TIMESTAMP DEFAULT GETDATE(),
    last_updated TIMESTAMP
);
"""

INSERT_STG_CRYPTOCURRENCIES_DATA = """
INSERT INTO STG_CRYPTOCURRENCIES_DATA (
    id, symbol, name, image, current_price_usd, market_cap,
    market_cap_short_number, market_cap_rank, fully_diluted_valuation,
    total_volume, high_24h, low_24h, price_change_24h, price_change_percentage_24h,
    market_cap_change_24h, market_cap_change_percentage_24h, circulating_supply,
    total_supply, max_supply, ath, ath_change_percentage, ath_date, atl,
    atl_change_percentage, atl_date, last_updated, dag_run_id
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

DELETE_TABLE_DIM_CRYPTOCURRENCIES = "DELETE DIM_CRYPTOCURRENCIES;"
DELETE_TABLE_FACTS_CRYPTOCURRENCIES = "DELETE FACTS_CRYPTOCURRENCIES;"
DELETE_TABLE_STG_CRYPTOCURRENCIES_DATA = "DELETE STG_CRYPTOCURRENCIES_DATA;"


class CryptoDataCollectorLoader(BaseOperator):
    """
    Operador Airflow para cargar criptomonedas en Redshift.
    """

    def __init__(self, *args, **kwargs):
        """Inicializa clase."""
        super().__init__(*args, **kwargs)
        self.input_path = ""
        self.connection_db = None

    def execute(self, context):
        """Ejecuta el proceso completo de carga de datos."""
        self.dag_run_id = context["run_id"]
        self.input_path = context['ti'].xcom_pull(task_ids='transform_crypto_data')
        self.connection_db = self.connect_to_db()
        self.create_tables()
        self.load_crypto_data()

    def connect_to_db(self):
        """Conecta a Redshift y retorna la conexión."""
        try:
            conn = redshift_connector.connect(
                host=os.environ['REDSHIFT_HOST'],
                database=os.environ['REDSHIFT_DATABASE'],
                user=os.environ['REDSHIFT_USER'],
                password=os.environ['REDSHIFT_PASSWORD'],
                port=int(os.getenv("REDSHIFT_PORT", 5439))
            )
        except Exception:
            raise AirflowSkipException("Error de conexión a la DB")

        cursor = conn.cursor()
        cursor.execute('SET search_path TO "2025_ignacio_gattei_schema";')
        conn.commit()
        return conn

    def create_tables(self):
        """Crea las tablas necesarias en Redshift."""
        cursor = self.connection_db.cursor()
        cursor.execute(CREATE_TABLE_DIM_CRYPTOCURRENCIES)
        cursor.execute(CREATE_TABLE_FACTS_CRYPTOCURRENCIES)
        cursor.execute(CREATE_TABLE_STG_CRYPTOCURRENCIES_DATA)
        self.connection_db.commit()
        cursor.close()

    def load_crypto_data(self):
        """Carga los datos del DataFrame en Redshift."""
        cursor = self.connection_db.cursor()
        df = pd.read_parquet(self.input_path)

        records = [
            (
                row.id, row.symbol, row.name, row.image, row.current_price,
                row.market_cap, format_number_short(row.market_cap),
                row.market_cap_rank, row.fully_diluted_valuation, row.total_volume,
                row.high_24h, row.low_24h, row.price_change_24h,
                row.price_change_percentage_24h, row.market_cap_change_24h,
                row.market_cap_change_percentage_24h, row.circulating_supply,
                row.total_supply, row.max_supply, row.ath, row.ath_change_percentage,
                row.ath_date, row.atl, row.atl_change_percentage, row.atl_date,
                row.last_updated, self.dag_run_id
            )
            for row in df.itertuples(index=False)
        ]

        cursor.executemany(INSERT_STG_CRYPTOCURRENCIES_DATA, records)
        self.log.info("Cantidad de registros cargados: %s", cursor.rowcount)
        self.connection_db.commit()
        cursor.close()

    def delete_tables(self):
        """Elimina las tablas de Redshift."""
        cursor = self.connection_db.cursor()
        cursor.execute(DELETE_TABLE_DIM_CRYPTOCURRENCIES)
        cursor.execute(DELETE_TABLE_FACTS_CRYPTOCURRENCIES)
        cursor.execute(DELETE_TABLE_STG_CRYPTOCURRENCIES_DATA)
        self.connection_db.commit()
        cursor.close()
