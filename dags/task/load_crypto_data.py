
import pandas as pd
import os
from datetime import datetime
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException
import redshift_connector
from utils.functions import format_number_short



CREATE_TABLE_STG_CRYPTOCURRENCIES_DATA = """
CREATE TABLE IF NOT EXISTS STG_CRYPTOCURRENCIES_DATA (
id VARCHAR(100),                                            -- ID crypto
symbol VARCHAR(100),                                        -- Simbolo de la crypto
name VARCHAR(500),                                          -- Nombre de la crypto
image VARCHAR(500),                                        -- Logo de la crypto
current_price_usd DECIMAL(38,18),                               -- Precio actual
market_cap DECIMAL(38,2),                                   -- Capitalización de mercado
market_cap_short_number VARCHAR(50),                         -- Capitalización de mercado (short number)
market_cap_rank INT,                                        -- Ranking de capitalización
fully_diluted_valuation BIGINT,                             -- Valoración diluida total
total_volume BIGINT,                                        -- Volumen total negociado
high_24h DECIMAL(38,18),                                     -- Máximo en 24h
low_24h DECIMAL(38,18),                                      -- Mínimo en 24h
price_change_24h DECIMAL(38,18),                             -- Cambio de precio 24h
price_change_percentage_24h DECIMAL(38,18),                  -- cambio precio 24h
market_cap_change_24h DECIMAL(38,2),                               -- Cambio de market cap 24h
market_cap_change_percentage_24h DECIMAL(38,2),             -- cambio market cap 24h
price_change_since_last_update DECIMAL(38,18),                  -- Cambio de precio desde la ultima novedad (CALCULADO)
price_change_percentage_since_last_update DECIMAL(38,18),         -- Cambio de precio en percent desde la ultima novedad (CALCULADO)
circulating_supply BIGINT,                           -- Suministro en circulación
total_supply BIGINT,                                 -- Suministro total
max_supply BIGINT,                                   -- Suministro máximo
ath DECIMAL(38,18),                                          -- Precio máximo histórico
ath_change_percentage DECIMAL(38,18),                        -- cambio desde el ATH
ath_date TIMESTAMP,                                         -- Fecha ATH
atl DECIMAL(38,18),                                          -- Precio mínimo histórico
atl_change_percentage DECIMAL(38,18),                        -- cambio desde el ATL
atl_date TIMESTAMP,                                         -- Fecha ATL
load_date TIMESTAMP DEFAULT GETDATE(),                       -- Fecha de carga en DW
last_updated TIMESTAMP,                                       -- Fecha de la ultima actualizacion de la cotizacion
dag_run_id VARCHAR(50)                                       -- ID de ejecucion del DAG
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
id VARCHAR(50),                                               -- ID crypto
current_price_usd DECIMAL(38,18),                            -- Precio actual en dolares
current_price_other_currency DECIMAL(38,18),                 -- Precio actual en otra moneda (CALCULADO)
other_currency VARCHAR(5),                                   -- Tipo de la otra moneda (CALCULADO)
market_cap DECIMAL(38,2),                                    -- Capitalización de mercado 
market_cap_short_number VARCHAR(50),                         -- Capitalización de mercado (short number) (CALCULADO)
market_cap_rank INT,                                        -- Ranking de capitalización
fully_diluted_valuation BIGINT,                             -- Valoración diluida total
total_volume BIGINT,                                        -- Volumen total negociado
high_24h DECIMAL(38,18),                                     -- Máximo en 24h
low_24h DECIMAL(38,18),                                      -- Mínimo en 24h
price_change_24h DECIMAL(38,18),                             -- Cambio de precio 24h
price_change_percentage_24h DECIMAL(38,18),                  -- Cambio precio en percent 24h
price_change_since_last_update DECIMAL(38,18),                  -- Cambio de precio desde la ultima novedad (CALCULADO)
price_change_percentage_since_last_update DECIMAL(38,18),         -- Cambio de precio en percent desde la ultima novedad (CALCULADO)
market_cap_change_24h DECIMAL(38,2),                               -- Cambio de market cap 24h
market_cap_change_percentage_24h DECIMAL(38,2),              -- cambio market cap 24h
circulating_supply BIGINT,                                   -- Suministro en circulación
total_supply BIGINT,                                        -- Suministro total
max_supply BIGINT,                                           -- Suministro máximo
ath DECIMAL(38,18),                                          -- Precio máximo histórico
ath_change_percentage DECIMAL(38,18),                        -- cambio desde el ATH
ath_date TIMESTAMP,                                         -- Fecha ATH
atl DECIMAL(38,18),                                          -- Precio mínimo histórico
atl_change_percentage DECIMAL(38,18),                        -- cambio desde el ATL
atl_date TIMESTAMP,                                         -- Fecha ATL
load_date TIMESTAMP DEFAULT GETDATE(),                       -- Fecha de carga en DW
last_updated TIMESTAMP                                        -- Fecha de la ultima actualizacion de la cotizacion
);
"""


INSERT_STG_CRYPTOCURRENCIES_DATA= """INSERT INTO STG_CRYPTOCURRENCIES_DATA (
id,symbol,name,image, current_price_usd, market_cap, market_cap_short_number, market_cap_rank, fully_diluted_valuation, 
total_volume, high_24h, low_24h, price_change_24h, price_change_percentage_24h,
market_cap_change_24h, market_cap_change_percentage_24h, circulating_supply,
total_supply, max_supply, ath, ath_change_percentage, ath_date, atl, atl_change_percentage, atl_date, last_updated,dag_run_id) 
VALUES ( %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

DELETE_TABLE_DIM_CRYPTOCURRENCIES = "DELETE DIM_CRYPTOCURRENCIES;"
DELETE_TABLE_FACTS_CRYPTOCURRENCIES = "DELETE FACTS_CRYPTOCURRENCIES;"
DELETE_TABLE_STG_CRYPTOCURRENCIES_DATA = "DELETE STG_CRYPTOCURRENCIES_DATA;"




class CryptoDataCollectorLoader(BaseOperator):
    def __init__(self,                  
                *args,**kwargs):
        """
        Inicializa clase
        """
        super(CryptoDataCollectorLoader, self).__init__(*args, **kwargs)   
        self.input_path = ''
        self.connection_db = None


    def execute(self, context):
        # dag_run_id : Para poder saber los datos que debo procesar en la tabla de staging dentro de mi dag run
        self.dag_run_id = context["run_id"]
        self.input_path = context['ti'].xcom_pull(task_ids='transform_crypto_data')
        df = pd.read_parquet(self.input_path)
        self.connection_db = self.connect_to_db()
        self.create_tables()
        self.load_crypto_data()


    def connect_to_db(self):
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
        return conn     
    
    def create_tables(self):
        cursor = self.connection_db.cursor()
        cursor.execute(CREATE_TABLE_DIM_CRYPTOCURRENCIES)    
        cursor.execute(CREATE_TABLE_FACTS_CRYPTOCURRENCIES)
        cursor.execute(CREATE_TABLE_STG_CRYPTOCURRENCIES_DATA)  
        self.connection_db.commit()  
        cursor.close()

    def load_crypto_data(self):
        cursor = self.connection_db.cursor()
        df = pd.read_parquet(self.input_path)


        cursor.executemany( INSERT_STG_CRYPTOCURRENCIES_DATA , [
        (
            row.id,
            row.symbol,
            row.name,
            row.image,
            row.current_price,
            row.market_cap,
            format_number_short(row.market_cap),
            row.market_cap_rank,
            row.fully_diluted_valuation,
            row.total_volume,
            row.high_24h,
            row.low_24h,
            row.price_change_24h,
            row.price_change_percentage_24h,
            row.market_cap_change_24h,
            row.market_cap_change_percentage_24h,
            row.circulating_supply,
            row.total_supply,
            row.max_supply,
            row.ath,
            row.ath_change_percentage,
            row.ath_date,
            row.atl,
            row.atl_change_percentage,
            row.atl_date,
            row.last_updated,
            self.dag_run_id 

        )
        for row in df.itertuples(index=False)
        ])

        print(f"Cant. registros cargados: {cursor.rowcount}")

        self.connection_db.commit()  
        cursor.close()


    def delete_tables(self):
        cursor = self.connection_db.cursor()
        cursor.execute(DELETE_TABLE_DIM_CRYPTOCURRENCIES)    
        cursor.execute(DELETE_TABLE_FACTS_CRYPTOCURRENCIES)
        cursor.execute(DELETE_TABLE_STG_CRYPTOCURRENCIES_DATA)  
        self.connection_db.commit()  
        cursor.close()

