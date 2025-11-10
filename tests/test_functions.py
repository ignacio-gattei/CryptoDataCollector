import pytest
import pandas as pd
from utils.functions import normalize_to_decimal,normalize_to_int,format_number_short
from pandas.testing import assert_frame_equal
from decimal import Decimal

def test_normalize_to_int_with_nulls():
    """Prueba con valores nulos y notación científica."""

    # Creamos un DataFrame con notación científica
    df = pd.DataFrame({
        'x': ['0.314e3', None, '5.23e5', '0','7389','2.1']  
    })

    # Aplicamos la función
    result = normalize_to_int(df.copy(), columns=['x'])

    # Definimos el resultado esperado
    expected = pd.DataFrame({
        'x': [314, None, 523000, 0, 7389, 2]
    })

    # Verificamos igualdad
    assert_frame_equal(result, expected)


def test_normalize_to_int_non_numeric_values():
    """Prueba que falle si hay valores no numéricos."""

    # DataFrame con un valor no convertible ('abc')
    df = pd.DataFrame({'a': ['10', 'abc', '30']})

    # Esperamos que falle con ValueError al intentar convertir 'abc'
    with pytest.raises(ValueError):
        normalize_to_int(df, columns=['a'])



def test_normalize_to_decimal_basic():
    """Prueba básica con números flotantes, enteros y nulos."""

# Creamos un DataFrame con diferentes valores
df = pd.DataFrame({
    'a': ['3.14256789', '2', None, '0.00052'],
    'b': ['1.535e-3','2.5e-8', None, '30.987654321']
})

# Ejecutamos la función sobre ambas columnas, con 4 decimales
result = normalize_to_decimal(df.copy(), decimals=4, columns=['a', 'b'])

# Definimos el DataFrame esperado: todos los valores convertidos a Decimal con 4 decimales
expected = pd.DataFrame({
    'a': [
        Decimal('3.1426'),  # redondeado a 4 decimales
        Decimal('2.0000'),
        None,
        Decimal('0.0005')
    ],
    'b': [
        Decimal('0.0015'),
        Decimal('0.0000'),
        None,
        Decimal('30.9877')
    ]
})

# Comparamos ambos DataFrames
assert_frame_equal(result, expected)

def test_normalize_to_decimal_basic():
    """Prueba básica con números flotantes, enteros y nulos."""

# Creamos un DataFrame con diferentes valores
df = pd.DataFrame({
    'a': ['3.14256789', '2', None, '0.00052'],
    'b': ['1.535e-3','2.5e-8', None, '30.987654321']
})

# Ejecutamos la función sobre ambas columnas, con 4 decimales
result = normalize_to_decimal(df.copy(), decimals=4, columns=['a', 'b'])

# Definimos el DataFrame esperado: todos los valores convertidos a Decimal con 4 decimales
expected = pd.DataFrame({
    'a': [
        Decimal('3.1426'),  # redondeado a 4 decimales
        Decimal('2.0000'),
        None,
        Decimal('0.0005')
    ],
    'b': [
        Decimal('0.0015'),
        Decimal('0.0000'),
        None,
        Decimal('30.9877')
    ]
})

# Comparamos ambos DataFrames
assert_frame_equal(result, expected)
    
def test_normalize_to_decimal_non_numeric_string():
    """Prueba que falle si se intenta convertir un valor no numérico."""

    # DataFrame con un valor no convertible a Decimal
    df = pd.DataFrame({'x': ['abc', '123']})

    # Debe lanzar InvalidOperation (o ValueError)
    with pytest.raises(Exception):
        normalize_to_decimal(df.copy(), columns=['x'])    



def test_format_number_short():
    """
    Verifica que la función formatee correctamente diferentes magnitudes numéricas.
    """
    # Valores grandes
    assert format_number_short(1520348589) == "1.52B"
    assert format_number_short(27244874229) == "27.24B"
    assert format_number_short(15432) == "15.43K"

    # Valores pequeños
    assert format_number_short(0.179627) == "0.18"

    # Valor None
    assert format_number_short(None) == "-"

    # Valor no numérico
    assert format_number_short("texto") == "-"
