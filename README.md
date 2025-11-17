
# Proyecto de Data Engineering - Crypto Data Collector con Airflow

####  Introducción

Este proyecto implementa un pipeline de datos en Apache Airflow para automatizar la consulta, el procesamiento y el almacenamiento de cotizaciones de criptomonedas.

El flujo extrae información desde APIs públicas, transforma los datos y los carga en un data warehouse en la nube (Amazon Redshift) para su posterior análisis.

Se implementaron agregaciones para calcular el top de capitalización de mercado de las principales criptomonedas y se realiza el cálculo de la variación frecuente de precios, lo que permite identificar casi en tiempo real aquellas criptomonedas que experimentan cambios significativos, útil para evaluar oportunidades o riesgos en estrategias de trading.

Además, se integró una API adicional que permite consultar el tipo de cambio en distintas monedas, posibilitando visualizar las cotizaciones de las criptomonedas en la divisa deseada.



## ETL

A continuación se detallan las tareas que componen el DAG principal del proceso ETL en Apache Airflow.

Estas tareas trabajan de forma orquestada para garantizar que la información se procese de manera automatizada, confiable y lista para análisis financiero.

#### `extract_crypto_data`
Obtiene las cotizaciones de las principales criptomonedas (Top 100) e información relevante como:
- Capitalización de mercado
- Volumen operado
- Máximos y mínimos históricos

Los datos se obtienen desde una API pública provista por CoinGecko.


#### `transform_crypto_data`
Normaliza los valores obtenidos por la API:
- Convierte números en notación científica a tipos Decimal o Integer
- Garantiza consistencia y precisión para su análisis

#### `load_crypto_data`
- Carga los datos ya transformados en una tabla de staging dentro del data warehouse.
- Creación automática de estructura de tablas si no existe
- Anade una nueva columna calculada, con la capitalizacion de mercado de la criptomoneda en forma corta y legible (Miles, Millones , Billones)




#### `extract_exchange_rate`
Consulta una API externa para obtener el tipo de cambio actualizado de las monedas indicadas (USD, EUR, ARS, etc.).



#### `transform_exchange_rate`
Prepara los datos del tipo de cambio para su carga:
- Formateo del cambio a decimal y preparacion de Dataframe listo para cargar en el Data warehouse
- Se agrega el nombre descriptivo de cada moneda como atributo adicional

#### `load_exchange_rate`
Carga los tipos de cambio al data warehouse, actualizando:
- Tablas de dimensiones
- Tablas de hechos relacionadas al tipo de cambio


#### `generate_summary_data`
Realiza las principales tareas de agregación y enriquecimiento analítico:

- Mueve los datos desde la tabla de staging hacia las entidades finales del modelo (DIM y FACTS)
- Calcula valores de las criptomonedas en otra diivisa, indicada en el proceso de extraccion de tipo de cambio.
- Genera un resumen de market cap de las principales criptomonedas agrupado por:
  - TOP 100 , TOP 50 ,TOP 20, TOP 10
- Calcula la variación de precio respecto del último valor procesado, esto permite detectar movimientos grandes de las cotizaciones de criptomonedas en casi tiempo real, útil para estrategias de trading.

  <br>

## Modelo Entidad–Relación (ERD)
 
El siguiente Modelo Entidad–Relación (ERD) representa la estructura lógica del Data Warehouse diseñado para el procesamiento y análisis de información de criptomonedas. 
Su objetivo es organizar los datos de forma óptima, garantizando integridad y consistencia  para consultas analíticas.

Este modelo define las entidades principales, sus atributos y las relaciones que existen entre ellas. De esta manera, se proporciona una vista clara del flujo y almacenamiento de los datos a lo largo del proceso ETL.


