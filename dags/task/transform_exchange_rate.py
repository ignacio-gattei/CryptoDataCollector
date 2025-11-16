import pandas as pd
import os
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException
from utils.functions import get_currency_name,to_decimal_2


class ExchangeRateTransformer(BaseOperator):
    template_fields = ("output_file_name",)
    def __init__(self,      
                output_path = "",
                output_file_name = "",
                *args,**kwargs):
        """
        Inicializa clase
        """
        super(ExchangeRateTransformer, self).__init__(*args, **kwargs)   
        self.output_path = output_path
        self.input_path = ''
        self.output_file_name = output_file_name


    def execute(self, context):
        self.output_path = os.path.join(self.output_path, self.output_file_name)
        self.input_path = context['ti'].xcom_pull(task_ids='extract_exchange_rate')
        source_currency_id = context['ti'].xcom_pull(task_ids='extract_exchange_rate', key='source_currency')
        target_currency_id = context['ti'].xcom_pull(task_ids='extract_exchange_rate', key='target_currency')
        df = pd.read_parquet(self.input_path)

        if len(df) != 1 or  df["moneda"].iloc[0] != source_currency_id:
         raise AirflowSkipException('Los datos a procesar son incorrectos')

        #Transformo y normalizo los datos
        df = pd.DataFrame({
        "source_currency_id": [source_currency_id],
        "source_currency_name": [get_currency_name(source_currency_id)],
        "target_currency_id": [target_currency_id],
        "target_currency_name": [get_currency_name(target_currency_id)],
        "exchange_rate": [to_decimal_2(df["venta"].iloc[0])],
        "exchange_rate_date": [df["fechaActualizacion"].iloc[0]]
        })


        df.to_parquet(self.output_path, index=False)
        print(f"Cant. registros transformados: {len(df)}")
        return self.output_path
    
