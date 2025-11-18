
# Proyecto de Data Engineering - Crypto Data Collector con Airflow

####  Introducción

Este proyecto implementa un pipeline de datos en Apache Airflow para automatizar la consulta, el procesamiento y el almacenamiento de cotizaciones de criptomonedas.

El flujo extrae información desde APIs públicas, transforma los datos y los carga en un data warehouse en la nube (Amazon Redshift) para su posterior análisis.

Se implementaron agregaciones para calcular el top de capitalización de mercado de las principales criptomonedas y se realiza el cálculo de la variación frecuente de precios, lo que permite identificar casi en tiempo real aquellas criptomonedas que experimentan cambios significativos, útil para evaluar oportunidades o riesgos en estrategias de trading.

Además, se integró una API adicional que permite consultar el tipo de cambio en distintas monedas, posibilitando visualizar las cotizaciones de las criptomonedas en la divisa deseada.



## ETL

A continuación se detallan las tareas que componen el DAG principal `dag_CryptoDataCollector` , del proceso ETL en Apache Airflow.

Estas tareas trabajan de forma orquestada para garantizar que la información se procese de manera automatizada, confiable y lista para análisis financiero.

#### Estructura del DAG

```
dags/
│
├── dag_CryptoDataCollector.py
│
└── task/
    ├── extract_crypto_data.py
    ├── transform_crypto_data.py
    ├── load_exchange_rate.py
    ├── transform_exchange_rate.py
    ├── transform_exchange_rate.py
    ├── load_exchange_rate.py
    └── generate_summary_data.py
```


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

![Diagrama ER](https://private-user-images.githubusercontent.com/240181618/515409862-8ac3c981-c4cd-4183-9497-48bd0b4a8717.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NjM0MTczOTgsIm5iZiI6MTc2MzQxNzA5OCwicGF0aCI6Ii8yNDAxODE2MTgvNTE1NDA5ODYyLThhYzNjOTgxLWM0Y2QtNDE4My05NDk3LTQ4YmQwYjRhODcxNy5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUxMTE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MTExN1QyMjA0NThaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT05NjkwYTc0ZmMzOTQ1MGJiNWVlYzNkY2VkOTQ1YmNjZmI4ZmU1ZjNjMWVlMzlkMjY0OGVlNzg1YTI5NmJlMjUwJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9._wES2beXyxX7LUX1_Vac_UdInrTkcrEhCvBvVOk6GHU)

  <br>

### Descripción de Tablas del Modelo

`STG_CRYPTOCURRENCIES_DATA`

Tabla de staging que almacena los datos crudos de criptomonedas obtenidos desde la API antes de ser transformados y enviados a las tablas finales.

`FACTS_CRYPTOCURRENCIES`

Tabla de hechos principal de cotizaciones de criptomonedas. Contiene precios,  variaciones y métricas del  mercado, incluyendo el precio  convertido a otras monedas.
Los datos de esta tabla se cargan desde la tabla de staging combinando datos con otras tablas del modelo, 
la task encargada de esto es `generate_summary_data`.

`DIM_CRYPTOCURRENCIES`

Dimensión que almacena la referencia estática de cada criptomoneda: id, símbolo, nombre e imagen asociada.

`FACTS_MARKET_CAP_RANK`

Hechos relacionados al ranking por capitalización del mercado. Permite hacer análisis por TOP 10, TOP 20, etc., para seguimiento de tendencias.

`FACTS_EXCHANGE_RATE`

Tabla de hechos que guarda los tipos de cambio entre distintas monedas, incluyendo fecha y valor de la conversión.
Los datos de esta tabla se cargan directamente desde la tarea de load programada desde Airflow.

`DIM_CURRENCY`

Dimensión que contiene la información de cada moneda utilizada para convertir las cotizaciones (ej.: USD, ARS, EUR).

<br>

### Atributos calculados

A continuación, se detallan los campos que son generados durante el proceso de transformación y carga

`price_change_since_last_update` : Calcula la variación absoluta del precio de una criptomoneda desde la última actualización almacenada.
Este indicador permite detectar movimientos bruscos en un corto período, siendo útil para identificar oportunidades o riesgos en estrategias de trading.

`price_change_percentage_since_last_update` : Determina la variación porcentual del precio respecto al valor anterior.
Ayuda a medir la intensidad del cambio de precio y compararlo entre diferentes criptomonedas.

`current_price_other_currency` : Indica el precio de la criptomoneda convertido a otra moneda (por ejemplo: ARS, EUR), facilitando el análisis según intereses o mercados específicos.

`market_cap_short_number` : Representa la capitalización de mercado en un formato abreviado y legible, sto simplifica la lectura en dashboards y reportes.
Ejemplos: K → miles, M → millones, B → billones.



`total_market_cap` : Valor agregado que resume la capitalización total de las criptomonedas agrupadas por rankings:
TOP 100, TOP 50, TOP 20 y TOP 10.
Permite evaluar fácilmente cómo se distribuye el valor del mercado cripto entre las principales.

  <br>

## Requisitos

Para ejecutar Apache Airflow utilizando Docker Compose, necesitás contar con:

- **Docker 28.5.1**  
- **Docker Compose v2.40.0**
- **Python v3.1**  
- **Sistema Operativo compatible** : Linux 

Librerias python utilizadas : numpy, pandas, redshift_connector, pytest

  
   <br>

## Ejecución del Proyecto

#### 1) Clonar el repositorio

```bash
git clone https://github.com/ignacio-gattei/CryptoDataCollector.git
cd CryptoDataCollector
```
#### 2) Configurar variables de entorno

Crear un archivo .env en la raíz del proyecto (si no está creado). Se proporcina un esqueleto del mismo en el repositorio para completar con las credenciales correspondientes.

#### 3) Ejecutar el proyecto con Makefile
Este proyecto incluye un Makefile que simplifica la ejecución:

Construir e iniciar todos los servicios

```bash
make run-project
```

Detener contenedor

```bash
make stop
```

Obtener Logs

```bash
make logs
```

Ejecutar pruebas unitarias

```bash
make test
```

#### 4) Acceso a la interfaz de Airflow

Una vez levantado el entorno, ingresar desde el navegador a:
```bash
http://localhost:8080
```

Inrgresar el Usuario y Contrasena proporcionadas


#### 5) Ejecutar ETL

Identificar al DAG `dag_CryptoDataCollector`
Encender y hacer un run del mismo como muestra la imagen.

<img width="1890" height="316" alt="Image" src="https://github.com/user-attachments/assets/4fe1fa60-f257-44c7-a1c2-d3d0b89752a8" />
