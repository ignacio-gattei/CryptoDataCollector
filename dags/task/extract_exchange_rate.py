import requests
import pandas as pd
import os
from datetime import datetime
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException


class ExchangeRateExtractor(BaseOperator):
    template_fields = ("output_file_name",)
    def __init__(self,
                api_endpoint = "",
                source_currency = "USD",
                target_currency = "ARS",
                output_file_name = "",
                output_path = "",
                *args,**kwargs):
        """
        Inicializa clase
        :param source_currency: moneda origen
        :param target_currency: moneda destino
        """
        super(ExchangeRateExtractor, self).__init__(*args, **kwargs)
        self.api_endpoint = api_endpoint
        self.source_currency = source_currency
        self.target_currency = target_currency
        self.output_file_name = output_file_name
        self.output_path = output_path


    def execute(self, context):
        self.output_path = os.path.join(self.output_path, self.output_file_name)
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
        response = requests.get(self.api_endpoint)
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
         

             
   
   