
# Proyecto de Data Engineering - Crypto Data Collector con Airflow

###  Introducción

Este proyecto implementa un pipeline de datos en Apache Airflow para automatizar la consulta, el procesamiento y el almacenamiento de cotizaciones de criptomonedas.

El flujo extrae información desde APIs públicas, transforma los datos y los carga en un data warehouse en la nube (Amazon Redshift) para su posterior análisis.

Se implementaron agregaciones para calcular el top de capitalización de mercado de las principales criptomonedas y se realiza el cálculo de la variación frecuente de precios, lo que permite identificar casi en tiempo real aquellas criptomonedas que experimentan cambios significativos, útil para evaluar oportunidades o riesgos en estrategias de trading.

Además, se integró una API adicional que permite consultar el tipo de cambio en distintas monedas, posibilitando visualizar las cotizaciones de las criptomonedas en la divisa deseada.


-----------

###  ETL implementado

El ETL implementado consta de una serie de tareas configuradas dentro de un DAG de Airflow, encargadas de la extracción, transformación y carga de datos.

Las tareas son las siguientes:

- **extract_crypto_data**: Obtiene las cotizaciones de las principales criptomonedas (top 100), así como información de capitalización, volumen operado, máximos y mínimos históricos, a partir de una API pública provista por CoinGecko.

- **transform_crypto_data**: Normaliza los valores obtenidos por la API, convirtiendo números en notación científica a tipos de datos Decimal o enteros, asegurando consistencia y precisión en los datos.

- **load_crypto_data**: Carga los datos previamente transformados en una tabla de staging dentro del data warehouse. Además, realiza una agregación simple para calcular la capitalización de mercado de forma legible, facilitando su análisis y lectura humana
