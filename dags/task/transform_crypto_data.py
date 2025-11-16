"""
Módulo para transformar y normalizar datos de criptomonedas
usando CryptoDataCollectorTransformer.
"""

import os

import pandas as pd
from airflow.models import BaseOperator

from utils.functions import normalize_to_decimal, normalize_to_int


class CryptoDataCollectorTransformer(BaseOperator):
    """
    Operador Airflow que transforma y normaliza datos de criptomonedas.
    """

    template_fields = ("output_file_name",)

    def __init__(
        self,
        *args,
        output_path="",
        output_file_name="",
        **kwargs,
    ):
        """
        Inicializa la clase.

        Args:
            output_path (str): Carpeta de salida para el archivo transformado.
            output_file_name (str): Nombre del archivo de salida.
        """
        super().__init__(*args, **kwargs)
        self.output_path = output_path
        self.output_file_name = output_file_name
        self.input_path = ""

    def execute(self, context):
        """
        Ejecuta la transformación de los datos de criptomonedas.

        Args:
            context (dict): Contexto de ejecución de Airflow.

        Returns:
            str: Ruta del archivo transformado.
        """
        self.input_path = context['ti'].xcom_pull(task_ids='extract_crypto_data')
        self.output_path = os.path.join(self.output_path, self.output_file_name)

        df = pd.read_parquet(self.input_path)

        # Normalizar columnas decimales
        df = normalize_to_decimal(
            df,
            18,
            columns=[
                "current_price",
                "high_24h",
                "low_24h",
                "price_change_24h",
                "price_change_percentage_24h",
                "market_cap_change_24h",
                "market_cap_change_percentage_24h",
                "ath",
                "ath_change_percentage",
                "atl",
                "atl_change_percentage",
            ],
        )

        # Normalizar columnas enteras
        df = normalize_to_int(
            df,
            columns=[
                "market_cap",
                "fully_diluted_valuation",
                "total_volume",
                "circulating_supply",
                "total_supply",
                "max_supply",
            ],
        )

        df.to_parquet(self.output_path, index=False)
        self.log.info("Cantidad de registros transformados: %d", len(df))

        return self.output_path
