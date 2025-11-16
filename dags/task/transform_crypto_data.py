import pandas as pd
import os
from datetime import datetime
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException
from decimal import Decimal, getcontext, InvalidOperation
from utils.functions import normalize_to_decimal,normalize_to_int


class CryptoDataCollectorTransformer(BaseOperator):
    template_fields = ("output_file_name",)
    def __init__(self,      
                output_path = "",
                output_file_name = "",
                *args,**kwargs):
        """
        Inicializa clase
        """
        super(CryptoDataCollectorTransformer, self).__init__(*args, **kwargs)   
        self.output_path = output_path
        self.input_path = ''



    def execute(self, context):
        self.output_path = os.path.join(self.output_path, self.output_file_name)
        self.input_path = context['ti'].xcom_pull(task_ids='extract_crypto_data')
        df = pd.read_parquet(self.input_path)

        df = normalize_to_decimal(df, 18, columns=["current_price", "high_24h","low_24h",
                                           "price_change_24h","price_change_percentage_24h",
                                           "market_cap_change_24h","market_cap_change_percentage_24h",
                                           "ath","ath_change_percentage","atl","atl_change_percentage"] )
        
        df = normalize_to_int(df, columns=["market_cap", "fully_diluted_valuation","total_volume",
                                                "circulating_supply","total_supply","max_supply"] )
        
        df.to_parquet(self.output_path, index=False)
        return self.output_path
    
