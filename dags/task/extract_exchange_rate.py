import requests
import pandas as pd
import time
import os
from datetime import datetime
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException


BASE_URL = "https://dolarapi.com/v1/dolares/bolsa"
FILE_DATA_NAME = "exchange_rate_extracted.parquet"

class ExchangeRateExtractor(BaseOperator):

    def __init__(self,
                source_currency = "USD",
                target_currency = "ARS",
                output_path = "",
                *args,**kwargs):
        """
        Inicializa clase
        :param source_currency: moneda origen
        :param target_currency: moneda destino
        """
        super(ExchangeRateExtractor, self).__init__(*args, **kwargs)
        self.source_currency = source_currency
        self.target_currency = target_currency
        self.output_path = os.path.join(output_path, FILE_DATA_NAME)


    def execute(self, context):
        if self.is_exchange_rate_supported() ==  False:
            raise AirflowSkipException('Tipo de cambio no soportado')
        json_data =  self.get_response()
        df = pd.DataFrame([json_data])
        df.to_parquet(self.output_path)
        if df.empty:
            raise AirflowSkipException('No se extrayeron datos')
        
        context['ti'].xcom_push(key='source_currency', value=self.source_currency)
        context['ti'].xcom_push(key='target_currency', value=self.target_currency)
        return self.output_path
    


    def get_response(self):
        response = requests.get(BASE_URL)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error {response.status_code} al consultar el tipo de cambio")
            return []

    def is_exchange_rate_supported(self):
         if self.source_currency == "USD" and self.target_currency == "ARS":
            return True
         else:
            return False
         

             
   
   