"""
Módulo para extraer datos de criptomonedas desde una API y guardarlos en parquet.
"""

import os
import time
from datetime import datetime

import pandas as pd
import requests
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException


class CryptoDataCollectorExtractor(BaseOperator):
    """
    Operador Airflow para descargar y almacenar datos de criptomonedas.
    """

    template_fields = ("output_file_name",)

    def __init__(
        self,
        *args,
        total_extract=100,
        api_endpoint="",
        output_path="",
        output_file_name="",
        **kwargs,
    ):
        """
        Inicializa clase.

        Args:
            total_extract (int): cantidad total de criptomonedas a obtener.
            api_endpoint (str): URL de la API.
            output_path (str): carpeta donde guardar el archivo.
            output_file_name (str): nombre del archivo de salida.
        """
        super().__init__(*args, **kwargs)
        self.api_endpoint = api_endpoint
        self.vs_currency = "usd"
        self.total = total_extract
        self.numb_reqs_until_waiting = 4
        self.per_page = min(250, total_extract)
        self.delay = 65
        self.order = "market_cap_desc"
        self.pages = total_extract // self.per_page
        self.data = []
        self.df = pd.DataFrame()
        self.output_path = output_path
        self.output_file_name = output_file_name

    def execute(self, context):
        """Ejecuta la extracción de datos y guarda el archivo parquet."""
        self.output_path = os.path.join(self.output_path, self.output_file_name)

        if os.path.exists(self.output_path):
            self.log.info("Reprocesamiento de archivo: %s", self.output_file_name)
        else:
            df = self.get_data_from_api()
            if df.empty:
                raise AirflowSkipException("No se extrayeron datos de la API")
            df.to_parquet(self.output_path)

        return self.output_path

    def get_page(self, page):
        """Obtiene una página de resultados de la API."""
        params = {
            "vs_currency": self.vs_currency,
            "order": self.order,
            "per_page": self.per_page,
            "page": page,
            "sparkline": "false",
            "price_change_percentage": "1h,24h,7d",
        }
        response = requests.get(self.api_endpoint, params=params)
        if response.status_code == 200:
            return response.json()
        self.log.warning("Error %d al descargar página %d",
                         response.status_code, page)
        return []

    def get_all_pages(self):
        """Descarga todas las páginas de criptomonedas."""
        i = 0
        self.log.info("Descargando top %d criptomonedas en %s...",
                      self.total, self.vs_currency.upper())

        for page in range(1, self.pages + 1):
            self.log.info("Página %d/%d", page, self.pages)
            data = self.get_page(page)
            self.data.extend(data)
            i += 1
            if i > self.numb_reqs_until_waiting:
                self.log.info("Esperando %d segundos...", self.delay)
                time.sleep(self.delay)
                i = 0

        self.log.info("Descarga completa: %d registros.", len(self.data))
        return self.data

    def get_data_from_api(self):
        """Descarga y convierte los datos de la API a DataFrame."""
        self.get_all_pages()
        self.convert_to_dataframe()
        self.log.info("Cant. registros extraidos desde la API: %d",
                      self.count_rows_df())
        return self.df

    def convert_to_dataframe(self):
        """Convierte los datos descargados a un DataFrame de pandas."""
        if not self.data:
            self.log.warning("No hay datos.")
            return None
        self.df = pd.DataFrame(self.data)
        return self.df

    def print_df(self, n=None):
        """Muestra las primeras n filas del DataFrame."""
        if self.df.empty:
            self.log.info("El DataFrame está vacío.")
            return
        if n is None:
            n = self.total
        self.log.info("\n%s", self.df.head(n))

    def save_csv(self, filename=None):
        """Guarda el DataFrame a un archivo CSV con fecha en el nombre."""
        if self.df.empty:
            self.log.info("El DataFrame está vacío.")
            return
        if not filename:
            fecha = datetime.now().strftime("%Y-%m-%d")
            filename = f"coingecko_top{self.total}_{self.vs_currency}_{fecha}.csv"
        self.df.to_csv(filename, index=False, encoding="utf-8-sig")
        self.log.info("Archivo guardado como: %s", filename)
        return filename

    def count_rows_df(self):
        """Devuelve e imprime la cantidad de filas del DataFrame."""
        if self.df.empty:
            self.log.info("El DataFrame está vacío.")
            return 0
        count = len(self.df)
        self.log.info("Cantidad de registros en el DataFrame: %d", count)
        return count
