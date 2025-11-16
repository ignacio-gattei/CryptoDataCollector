import pandas as pd
from datetime import datetime
from decimal import Decimal, getcontext, InvalidOperation, ROUND_HALF_UP
from typing import List, Optional, Union


def normalize_to_decimal(
    df: pd.DataFrame,
    decimals: int = 18,
    columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Convierte columnas numéricas o seleccionadas de un DataFrame
    a Decimal con precisión segura para Redshift.

    Parámetros
    ----------
    df : pandas.DataFrame
        DataFrame de entrada.
    columns : list[str] | None
        Lista de columnas a convertir. Si es None, convierte todas las numéricas.
    decimals : int
        Cantidad de decimales a conservar (por defecto 18).

    Retorna
    -------
    pandas.DataFrame
        DataFrame con las columnas convertidas a Decimal.
    """
    getcontext().prec = 40

    if columns is None:
        columns = df.select_dtypes(include=["float", "int"]).columns.tolist()

    scale = Decimal("0." + "0" * (decimals - 1) + "1")

    for col in columns:
        df[col] = df[col].apply(
            lambda x: Decimal(str(x)).quantize(scale) if pd.notnull(x) else None
        )

    return df


def normalize_to_int(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Convierte columnas seleccionadas de un DataFrame a int/BIGINT,
    soportando notación científica y valores nulos.

    Parámetros
    ----------
    df : pandas.DataFrame
        DataFrame de entrada.
    columns : list[str]
        Lista de columnas a convertir a int/BIGINT.

    Retorna
    -------
    pandas.DataFrame
        DataFrame con las columnas convertidas a int.
    """
    for col in columns:
        df[col] = df[col].apply(lambda x: None if pd.isnull(x) else int(float(x)))
    return df


def format_number_short(num: Union[int, float, None], decimales: int = 2) -> str:
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
    if num is None:
        return "-"

    try:
        num = float(num)
    except (TypeError, ValueError):
        return "-"

    abs_num = abs(num)

    if abs_num >= 1_000_000_000_000:
        return f"{num / 1_000_000_000_000:.{decimales}f}T"
    if abs_num >= 1_000_000_000:
        return f"{num / 1_000_000_000:.{decimales}f}B"
    if abs_num >= 1_000_000:
        return f"{num / 1_000_000:.{decimales}f}M"
    if abs_num >= 1_000:
        return f"{num / 1_000:.{decimales}f}K"

    return f"{num:.{decimales}f}"


def union_2_columns_to_1_column(
    df: pd.DataFrame, col1: str, col2: str, new_col_name: str = "combined"
) -> pd.DataFrame:
    """
    Une los valores de dos columnas de un DataFrame en una sola columna.

    Parámetros
    ----------
    df : pandas.DataFrame
        DataFrame de entrada.
    col1 : str
        Nombre de la primera columna.
    col2 : str
        Nombre de la segunda columna.
    new_col_name : str
        Nombre de la columna resultante (por defecto 'combined').

    Retorna
    -------
    pandas.DataFrame
        DataFrame con la columna combinada.
    """
    combined = pd.concat([df[col1], df[col2]], ignore_index=True)
    return pd.DataFrame({new_col_name: combined})


def get_currency_name(symbol: str) -> str:
    """
    Retorna el nombre completo de una moneda según su símbolo.

    Parámetros
    ----------
    symbol : str
        Símbolo de la moneda (por ejemplo: 'USD', 'EUR').

    Retorna
    -------
    str
        Nombre completo de la moneda o 'Unknown currency' si no se reconoce.
    """
    currency_names = {
        "USD": "US Dollar",
        "EUR": "Euro",
        "ARS": "Argentine Peso",
        "GBP": "British Pound",
        "JPY": "Japanese Yen",
        "CNY": "Chinese Yuan",
        "CAD": "Canadian Dollar",
        "AUD": "Australian Dollar",
        "CHF": "Swiss Franc",
        "MXN": "Mexican Peso",
        "BRL": "Brazilian Real",
    }

    return currency_names.get(symbol.upper().strip(), "Unknown currency")


def to_decimal_2(value: Union[str, int, float, Decimal]) -> Optional[Decimal]:
    """
    Convierte un valor a Decimal con 2 decimales.

    Parámetros
    ----------
    value : str | int | float | Decimal
        Valor a convertir.

    Retorna
    -------
    Decimal | None
        Valor convertido a Decimal con 2 decimales, o None si el valor no es válido.
    """
    if value is None:
        return None

    try:
        decimal_value = Decimal(str(value))
        decimal_value = decimal_value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return decimal_value
    except (InvalidOperation, ValueError, TypeError):
        return None
