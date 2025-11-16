"""
Operador Airflow para cargar tipos de cambio en Redshift.
Crea tablas de hecho y dimensión, carga datos y permite eliminar tablas.
"""

import os
import pandas as pd
import redshift_connector
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException
from utils.functions import union_2_columns_to_1_column

TABLE_FACTS_EXCHANGE_RATE = """
CREATE TABLE IF NOT EXISTS FACTS_EXCHANGE_RATE (
    source_currency_id VARCHAR(5),
    target_currency_id VARCHAR(5),
    exchange_rate DECIMAL(16,2),
    exchange_rate_date TIMESTAMP,
    load_date TIMESTAMP DEFAULT GETDATE()
);
"""

TABLE_DIM_CURRENCY = """
CREATE TABLE IF NOT EXISTS DIM_CURRENCY (
    currency_id VARCHAR(5),
    currency_name VARCHAR(50),
    created_date TIMESTAMP DEFAULT GETDATE()
);
"""

INSERT_TABLE_FACTS_EXCHANGE_RATE = """
INSERT INTO FACTS_EXCHANGE_RATE 
(source_currency_id, target_currency_id, exchange_rate, exchange_rate_date) 
VALUES (%s, %s, %s, %s)
"""

INSERT_TABLE_DIM_CURRENCY = """
INSERT INTO DIM_CURRENCY (currency_id, currency_name)
SELECT %s, %s
WHERE NOT EXISTS (
    SELECT 1 FROM DIM_CURRENCY WHERE currency_id = %s
);
"""

DROP_TABLE_DIM_CURRENCY = "DROP TABLE IF EXISTS DIM_CURRENCY;"
DROP_TABLE_FACTS_EXCHANGE_RATE = "DROP TABLE IF EXISTS FACTS_EXCHANGE_RATE;"


class ExchangeRateLoader(BaseOperator):
    """Operador Airflow para cargar tipos de cambio en Redshift."""

    def __init__(self, *args, **kwargs):
        """Inicializa el operador."""
        super().__init__(*args, **kwargs)
        self.input_path = ""
        self.connection_db = None

    def execute(self, context: dict):
        """Ejecuta la carga de datos."""
        self.input_path = context['ti'].xcom_pull(task_ids='transform_exchange_rate')
        self.connection_db = self.connect_to_db()
        self.create_tables()
        self.load_data()

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
        """Crea las tablas de hecho y dimensión."""
        cursor = self.connection_db.cursor()
        cursor.execute(TABLE_DIM_CURRENCY)
        cursor.execute(TABLE_FACTS_EXCHANGE_RATE)
        self.connection_db.commit()
        cursor.close()

    def drop_tables(self):
        """Elimina las tablas de Redshift."""
        cursor = self.connection_db.cursor()
        cursor.execute(DROP_TABLE_DIM_CURRENCY)
        cursor.execute(DROP_TABLE_FACTS_EXCHANGE_RATE)
        self.connection_db.commit()
        cursor.close()

    def load_data(self):
        """Carga los datos desde el parquet a Redshift."""
        cursor = self.connection_db.cursor()
        df = pd.read_parquet(self.input_path)

        df_currency_id = union_2_columns_to_1_column(
            df, "source_currency_id", "target_currency_id", "currency_id"
        )
        df_currency_name = union_2_columns_to_1_column(
            df, "source_currency_name", "target_currency_name", "currency_name"
        )
        pd_currency = pd.concat(
            [df_currency_id.reset_index(drop=True),
             df_currency_name.reset_index(drop=True)],
            axis=1
        )

        cursor.executemany(
            INSERT_TABLE_DIM_CURRENCY,
            [
                (row.currency_id, row.currency_name, row.currency_id)
                for row in pd_currency.itertuples(index=False)
            ]
        )

        cursor.executemany(
            INSERT_TABLE_FACTS_EXCHANGE_RATE,
            [
                (
                    row.source_currency_id,
                    row.target_currency_id,
                    row.exchange_rate,
                    row.exchange_rate_date
                )
                for row in df.itertuples(index=False)
            ]
        )

        self.connection_db.commit()
        cursor.close()
