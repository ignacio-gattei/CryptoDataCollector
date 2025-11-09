import requests
import pandas as pd
import time
import os
from datetime import datetime
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException
import redshift_connector
from decimal import Decimal, getcontext, InvalidOperation

FILE_DATA_NAME = "crypto_data_transformed.parquet"

TABLE_CRYPTOCURRENCIES = """
CREATE TABLE IF NOT EXISTS CRYPTOCURRENCIES (
id VARCHAR(100) PRIMARY KEY,
symbol VARCHAR(100),
name VARCHAR(500),
image VARCHAR(1000)
);
"""

TABLE_CRYPTOCURRENCIES_FACTS = """
CREATE TABLE IF NOT EXISTS CRYPTOCURRENCIES_FACTS (
id VARCHAR(100),                               -- ID crypto
current_price DECIMAL(38,18),                                -- Precio actual
market_cap BIGINT,                                          -- Capitalización de mercado
market_cap_rank INT,                                        -- Ranking de capitalización
fully_diluted_valuation BIGINT,                             -- Valoración diluida total
total_volume BIGINT,                                        -- Volumen total negociado
high_24h DECIMAL(38,18),                                     -- Máximo en 24h
low_24h DECIMAL(38,18),                                      -- Mínimo en 24h
price_change_24h DECIMAL(38,18),                             -- Cambio de precio 24h
price_change_percentage_24h DECIMAL(38,18),                  -- cambio precio 24h
market_cap_change_24h DECIMAL(38,18),                               -- Cambio de market cap 24h
market_cap_change_percentage_24h DECIMAL(38,18),             -- cambio market cap 24h
circulating_supply DECIMAL(38,18),                           -- Suministro en circulación
total_supply BIGINT,                                 -- Suministro total
max_supply BIGINT,                                   -- Suministro máximo
ath DECIMAL(38,18),                                          -- Precio máximo histórico
ath_change_percentage DECIMAL(38,18),                        -- cambio desde el ATH
ath_date TIMESTAMP,                                         -- Fecha ATH
atl DECIMAL(38,18),                                          -- Precio mínimo histórico
atl_change_percentage DECIMAL(38,18),                        -- cambio desde el ATL
atl_date TIMESTAMP,                                         -- Fecha ATL
load_date TIMESTAMP DEFAULT GETDATE()                       -- Fecha de carga en DW
);
"""

INSERT_CRYPTOCURRENCIES = """INSERT INTO CRYPTOCURRENCIES (id, symbol, name, image) VALUES (%s, %s, %s, %s)"""

INSERT_CRYPTOCURRENCIES_FACTS = """INSERT INTO CRYPTOCURRENCIES_FACTS (
id, current_price, market_cap, market_cap_rank, fully_diluted_valuation, 
total_volume, high_24h, low_24h, price_change_24h, price_change_percentage_24h,
market_cap_change_24h, market_cap_change_percentage_24h, circulating_supply,
total_supply, max_supply, ath, ath_change_percentage, ath_date, atl, atl_change_percentage, atl_date) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""



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
        self.input_path = context['ti'].xcom_pull(task_ids='transform_crypto_data')
        df = pd.read_parquet(self.input_path)
        self.connection_db = self.connect_to_db()
        self.drop_tables()
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
        cursor.execute(TABLE_CRYPTOCURRENCIES)    
        cursor.execute(TABLE_CRYPTOCURRENCIES_FACTS)  
        self.connection_db.commit()  
        cursor.close()


    def drop_tables(self):
        cursor = self.connection_db.cursor()
        cursor.execute("DROP TABLE IF EXISTS CRYPTOCURRENCIES;")
        cursor.execute("DROP TABLE IF EXISTS CRYPTOCURRENCIES_FACTS;") 
        self.connection_db.commit()  
        cursor.close()


    def load_crypto_data(self):
        cursor = self.connection_db.cursor()
        df = pd.read_parquet(self.input_path)


        cursor.executemany( INSERT_CRYPTOCURRENCIES , [
            ( row.id,row.symbol,row.name,row.image) for row in df.itertuples(index=False)
        ])


        cursor.executemany( INSERT_CRYPTOCURRENCIES_FACTS , [
        (
            row.id,
            row.current_price,
            row.market_cap,
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
            row.atl_date
        )
        for row in df.itertuples(index=False)
        ])


        self.connection_db.commit()  
        cursor.close()

    
    def normalize_decimal(self, df, decimals=18,  columns=None):
        """
        Convierte columnas numéricas o seleccionadas de un DataFrame
        a Decimal(38,decimals) con precisión segura para Redshift.
        
        Parámetros:
        -----------
        df : pandas.DataFrame
            DataFrame de entrada.
        columns : list[str] | None
            Lista de columnas a convertir. Si es None, convierte todas las numéricas.
        decimals : int
            Cantidad de decimales a conservar (por defecto 18).
            
        Retorna:
        --------
        pandas.DataFrame con las columnas convertidas a Decimal.
        """
        # Precisión global alta
        getcontext().prec = 40
        
        # Si no se especifican columnas, detectar las numéricas
        if columns is None:
            columns = df.select_dtypes(include=["float", "int"]).columns.tolist()
        
        quantizer = Decimal("1." + "0" * decimals)
        scale = Decimal("0." + "0" * (decimals - 1) + "1")

        for col in columns:
            df[col] = df[col].apply(lambda x: (
                Decimal(str(x)).quantize(scale)
                if pd.notnull(x) else None
            ))

        return df