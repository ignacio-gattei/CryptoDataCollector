import pandas as pd
from datetime import datetime
from decimal import Decimal, getcontext

def normalize_to_decimal( df, decimals=18,  columns=None):
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

def normalize_to_int(df,columns=None):
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