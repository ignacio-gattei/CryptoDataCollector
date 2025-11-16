"""
Extractor de tipo de cambio entre monedas utilizando una API y
almacenamiento en parquet.
"""

import os

import pandas as pd
import requests
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException


class ExchangeRateExtractor(BaseOperator):
    """
    Operador Airflow para extraer el tipo de cambio entre monedas
    soportadas y guardarlo en archivo parquet.
    """

    template_fields = ("output_file_name",)

    def __init__(
        self,
        *args,
        api_endpoint="",
        source_currency="USD",
        target_currency="ARS",
        output_file_name="",
        output_path="",
        **kwargs,
    ):
        """
        Inicializa clase.

        Args:
            api_endpoint (str): URL de la API.
            source_currency (str): moneda origen.
            target_currency (str): moneda destino.
            output_file_name (str): nombre del archivo de salida.
            output_path (str): ruta de salida del archivo.
        """
        super().__init__(*args, **kwargs)
        self.api_endpoint = api_endpoint
        self.source_currency = source_currency
        self.target_currency = target_currency
        self.output_file_name = output_file_name
        self.output_path = output_path

    def execute(self, context):
        """
        Ejecuta la extracción del tipo de cambio y guarda el archivo.
        """
        if not self.is_exchange_rate_supported():
            raise AirflowSkipException("Tipo de cambio no soportado")

        self.output_path = os.path.join(self.output_path, self.output_file_name)

        if os.path.exists(self.output_path):
            self.log.info(
                "Reprocesamiento de archivo: %s", self.output_file_name
            )
        else:
            df = self.get_data_from_api()
            if df.empty:
                raise AirflowSkipException("No se extrayeron datos de la API")
            df.to_parquet(self.output_path)

        context['ti'].xcom_push(key='source_currency', value=self.source_currency)
        context['ti'].xcom_push(key='target_currency', value=self.target_currency)

        return self.output_path

    def get_response(self):
        """
        Obtiene la respuesta JSON desde la API.
        """
        response = requests.get(self.api_endpoint)
        if response.status_code == 200:
            return response.json()

        self.log.warning(
            "Error %d al consultar el tipo de cambio", response.status_code
        )
        return {}

    def get_data_from_api(self):
        """
        Convierte la respuesta de la API en un DataFrame.
        """
        json_data = self.get_response()
        df = pd.json_normalize(json_data)

        if not json_data:
            self.log.info("Cant. registros extraidos desde la API: 0")
        else:
            self.log.info(
                "Cant. registros extraidos desde la API: %d", len(df)
            )

        return df

    def is_exchange_rate_supported(self):
        """
        Verifica si la combinación de monedas es soportada.
        """
        return self.source_currency == "USD" and self.target_currency == "ARS"
