
# Proyecto de Data Engineering - Crypto Data Collector con Airflow

###  Introducción

Este proyecto implementa un pipeline de datos en Apache Airflow para automatizar la consulta, el procesamiento y el almacenamiento de cotizaciones de criptomonedas.

El flujo extrae información desde APIs públicas, transforma los datos y los carga en un data warehouse en la nube (Amazon Redshift) para su posterior análisis.

Se implementaron agregaciones para calcular el top de capitalización de mercado de las principales criptomonedas y se realiza el cálculo de la variación frecuente de precios, lo que permite identificar casi en tiempo real aquellas criptomonedas que experimentan cambios significativos, útil para evaluar oportunidades o riesgos en estrategias de trading.

Además, se integró una API adicional que permite consultar el tipo de cambio en distintas monedas, posibilitando visualizar las cotizaciones de las criptomonedas en la divisa deseada.
