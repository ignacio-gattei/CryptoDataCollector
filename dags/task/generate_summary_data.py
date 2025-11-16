"""
Operador Airflow para generar resumen de criptomonedas en Redshift.
Incluye creación de tablas, inserción de datos y agregaciones.
"""

import os

import redshift_connector
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException


INSERT_DIM_CRYPTOCURRENCIES = """
INSERT INTO DIM_CRYPTOCURRENCIES (id, symbol, name, image)
SELECT C.id, C.symbol, C.name, C.image
FROM STG_CRYPTOCURRENCIES_DATA C
WHERE NOT EXISTS (
    SELECT 1 FROM DIM_CRYPTOCURRENCIES D WHERE D.id = C.id
);
"""

CREATE_TABLE_FACTS_MARKET_CAP_RANK = """
CREATE TABLE IF NOT EXISTS FACTS_MARKET_CAP_RANK (
    ranking VARCHAR(10),
    rank_date TIMESTAMP,
    total_market_cap DECIMAL(38,2),
    load_date TIMESTAMP DEFAULT GETDATE()
);
"""

AGGREGATE_PRICE_VARIATION = """
WITH LAST_FACTS_CRYPTOCURRENCIES AS (
    SELECT
        id,
        current_price_usd AS last_price,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_updated DESC) AS rn
    FROM FACTS_CRYPTOCURRENCIES
)
UPDATE STG_CRYPTOCURRENCIES_DATA
SET
    price_change_since_last_update = (current_price_usd - L.last_price),
    price_change_percentage_since_last_update = CASE
        WHEN L.last_price IS NULL OR L.last_price = 0 THEN NULL
        ELSE ((current_price_usd - L.last_price) / L.last_price) * 100
    END
FROM LAST_FACTS_CRYPTOCURRENCIES AS L
WHERE STG_CRYPTOCURRENCIES_DATA.id = L.id
  AND L.rn = 1
  AND STG_CRYPTOCURRENCIES_DATA.dag_run_id = %s;
"""

INSERT_FACTS_CRYPTOCURRENCIES = """
INSERT INTO FACTS_CRYPTOCURRENCIES (
    id, current_price_usd, current_price_other_currency, other_currency,
    market_cap, market_cap_short_number, market_cap_rank,
    fully_diluted_valuation, total_volume, high_24h, low_24h,
    price_change_24h, price_change_percentage_24h,
    price_change_since_last_update, price_change_percentage_since_last_update,
    market_cap_change_24h, market_cap_change_percentage_24h,
    circulating_supply, total_supply, max_supply,
    ath, ath_change_percentage, ath_date,
    atl, atl_change_percentage, atl_date, last_updated
)
SELECT
    C.id,
    C.current_price_usd,
    C.current_price_usd * ER.exchange_rate AS current_price_other_currency,
    ER.target_currency_id AS other_currency,
    C.market_cap,
    C.market_cap_short_number,
    C.market_cap_rank,
    C.fully_diluted_valuation,
    C.total_volume,
    C.high_24h,
    C.low_24h,
    C.price_change_24h,
    C.price_change_percentage_24h,
    C.price_change_since_last_update,
    C.price_change_percentage_since_last_update,
    C.market_cap_change_24h,
    C.market_cap_change_percentage_24h,
    C.circulating_supply,
    C.total_supply,
    C.max_supply,
    C.ath,
    C.ath_change_percentage,
    C.ath_date,
    C.atl,
    C.atl_change_percentage,
    C.atl_date,
    C.last_updated
FROM STG_CRYPTOCURRENCIES_DATA C
CROSS JOIN FACTS_EXCHANGE_RATE ER
WHERE C.dag_run_id = %s
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY C.id
    ORDER BY ABS(EXTRACT(EPOCH FROM (C.last_updated - ER.exchange_rate_date)))
) = 1;
"""

INSERT_FACTS_MARKET_CAP_RANK = """
INSERT INTO FACTS_MARKET_CAP_RANK (ranking, rank_date, total_market_cap)
SELECT
    CASE
        WHEN market_cap_rank <= 10 THEN 'TOP 10'
        WHEN market_cap_rank <= 20 THEN 'TOP 20'
        WHEN market_cap_rank <= 50 THEN 'TOP 50'
        WHEN market_cap_rank <= 100 THEN 'TOP 100'
        ELSE 'TOP +100'
    END AS ranking,
    MAX(last_updated) AS rank_date,
    SUM(market_cap) AS total_market_cap
FROM STG_CRYPTOCURRENCIES_DATA
WHERE dag_run_id = %s
GROUP BY 1;
"""

DELETE_FACTS_MARKET_CAP_RANK = "DELETE FROM FACTS_MARKET_CAP_RANK;"
DEPURATE_STG_CRYPTOCURRENCIES = """
DELETE FROM STG_CRYPTOCURRENCIES_DATA WHERE dag_run_id = %s;
"""


class SummaryGenerator(BaseOperator):
    """
    Operador Airflow para generar resumen de criptomonedas en Redshift.
    """

    def __init__(self, *args, **kwargs):
        """Inicializa clase"""
        super().__init__(*args, **kwargs)
        self.connection_db = None

    def execute(self, context):
        """Ejecuta todo el proceso de agregación y carga"""
        self.dag_run_id = context["run_id"]
        self.connection_db = self.connect_to_db()
        self.create_tables()
        self.aggregate_crypto_data()

    def connect_to_db(self):
        """Conecta a Redshift y retorna la conexión"""
        try:
            conn = redshift_connector.connect(
                host=os.environ['REDSHIFT_HOST'],
                database=os.environ['REDSHIFT_DATABASE'],
                user=os.environ['REDSHIFT_USER'],
                password=os.environ['REDSHIFT_PASSWORD'],
                port=int(os.getenv("REDSHIFT_PORT", 5439)),
            )
        except Exception:
            raise AirflowSkipException("Error de conexión a la DB")

        cursor = conn.cursor()
        cursor.execute('SET search_path TO "2025_ignacio_gattei_schema";')
        conn.autocommit = True
        conn.commit()
        return conn

    def create_tables(self):
        """Crea las tablas necesarias en Redshift."""
        cursor = self.connection_db.cursor()
        cursor.execute(CREATE_TABLE_FACTS_MARKET_CAP_RANK)
        self.connection_db.commit()
        cursor.close()

    def aggregate_crypto_data(self):
        """Ejecuta todos los inserts y agregaciones en Redshift."""
        cursor = self.connection_db.cursor()
        cursor.execute(INSERT_DIM_CRYPTOCURRENCIES)
        cursor.execute(AGGREGATE_PRICE_VARIATION, (self.dag_run_id,))
        cursor.execute(INSERT_FACTS_CRYPTOCURRENCIES, (self.dag_run_id,))
        cursor.execute(INSERT_FACTS_MARKET_CAP_RANK, (self.dag_run_id,))
        cursor.execute(DEPURATE_STG_CRYPTOCURRENCIES, (self.dag_run_id,))
        self.log.info("Procesado dag_run_id: %s", self.dag_run_id)
        self.connection_db.commit()
        cursor.close()

    def delete_tables(self):
        """Elimina las tablas de resumen en Redshift."""
        cursor = self.connection_db.cursor()
        cursor.execute(DELETE_FACTS_MARKET_CAP_RANK)
        self.connection_db.commit()
        cursor.close()
