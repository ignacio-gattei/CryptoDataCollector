import requests
import pandas as pd
import time
import os
from datetime import datetime
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException
from decimal import Decimal, getcontext, InvalidOperation

FILE_DATA_NAME = "crypto_data_transformed.parquet"


class CryptoDataCollectorTransformer(BaseOperator):
    def __init__(self,      
                output_path = "",
                *args,**kwargs):
        """
        Inicializa clase
        """
        super(CryptoDataCollectorTransformer, self).__init__(*args, **kwargs)   
        self.output_path = os.path.join(output_path , FILE_DATA_NAME)
        self.input_path = ''



    def execute(self, context):
        self.input_path = context['ti'].xcom_pull(task_ids='extract_crypto_data')
        df = pd.read_parquet(self.input_path)

        df = self.normalize_to_decimal(df, 18, columns=["current_price", "high_24h","low_24h",
                                           "price_change_24h","price_change_percentage_24h",
                                           "market_cap_change_24h","market_cap_change_percentage_24h",
                                           "ath","ath_change_percentage","atl","atl_change_percentage"] )
        
        df = self.normalize_to_int(df, columns=["market_cap", "fully_diluted_valuation","total_volume",
                                                "circulating_supply","total_supply","max_supply"] )
        
        df.to_parquet(self.output_path, index=False)
        return self.output_path
    



    def normalize_to_decimal(self, df, decimals=18,  columns=None):
        """
        Convierte columnas numéricas o seleccionadas de un DataFrame
        a Decimal(38,decimals) con precisión segura para Redshift.
        
        Parámetros:
        -----------
        df : pandas.DataFrame
            DataFrame de entrada.
        columns : list[str] | None
            Lista de columnas a convertir. Si es None, convierte todas las numéricas.
        decimals : int
            Cantidad de decimales a conservar (por defecto 18).
            
        Retorna:
        --------
        pandas.DataFrame con las columnas convertidas a Decimal.
        """
        # Precisión global alta
        getcontext().prec = 40
        
        # Si no se especifican columnas, detectar las numéricas
        if columns is None:
            columns = df.select_dtypes(include=["float", "int"]).columns.tolist()
        
        quantizer = Decimal("1." + "0" * decimals)
        scale = Decimal("0." + "0" * (decimals - 1) + "1")

        for col in columns:
            df[col] = df[col].apply(lambda x: (
                Decimal(str(x)).quantize(scale)
                if pd.notnull(x) else None
            ))

        return df
    
    def normalize_to_int(self,df,columns=None):
        """
        Convierte las columnas seleccionadas de un DataFrame a BIGINT (int en Python),
        soportando notación científica y valores nulos.
        
        Parámetros:
        -----------
        df : pandas.DataFrame
            DataFrame de entrada.
        columns : list[str]
            Lista de columnas a convertir a BIGINT.
            
        Retorna:
        --------
        pandas.DataFrame con las columnas convertidas a int/BIGINT.
        """
        for col in columns:
            df[col] = df[col].apply(lambda x: None if pd.isnull(x) else int(float(x)))
        return df