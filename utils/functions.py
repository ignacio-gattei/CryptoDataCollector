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

def format_number_short(num, decimales=2):
    """
    Convierte un número grande en un formato legible con sufijos (K, M, B, T).

    Parámetros
    ----------
    num : int | float | None
        Número a formatear.
    decimales : int
        Cantidad de decimales a mostrar (por defecto 2).

    Retorna
    -------
    str
        Cadena formateada con el sufijo correspondiente.
    """
    # Manejo de valores nulos o no numéricos
    if num is None:
        return "-"
    
    try:
        num = float(num)
    except (TypeError, ValueError):
        return "-"

    abs_num = abs(num)

    # Determinar el sufijo adecuado según la magnitud del número
    if abs_num >= 1_000_000_000_000:
        return f"{num / 1_000_000_000_000:.{decimales}f}T"
    elif abs_num >= 1_000_000_000:
        return f"{num / 1_000_000_000:.{decimales}f}B"
    elif abs_num >= 1_000_000:
        return f"{num / 1_000_000:.{decimales}f}M"
    elif abs_num >= 1_000:
        return f"{num / 1_000:.{decimales}f}K"
    else:
        return f"{num:.{decimales}f}"
    

def union_2_columns_to_1_column(df, col1, col2, new_col_name="combined"):
    """
    Une los valores de dos columnas de un DataFrame en una sola columna,
    similar a la operación UNION en SQL (una debajo de otra).

    Parámetros
    ----------
    df : pandas.DataFrame
        DataFrame de entrada.
    col1 : str
        Nombre de la primera columna.
    col2 : str
        Nombre de la segunda columna.

    Retorna
    -------
    pandas.Series
        Columna única con los valores combinados de ambas columnas.
    """
    combined = pd.concat([df[col1], df[col2]], ignore_index=True)

     # Creamos un nuevo DataFrame con la columna combinada y el nombre indicado
    result_df = pd.DataFrame({new_col_name: combined})

    # Devolvemos la serie combinada
    return result_df