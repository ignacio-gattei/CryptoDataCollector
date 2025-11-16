import pytest
import pandas as pd
from decimal import Decimal
from pandas.testing import assert_frame_equal
from utils.functions import (
    normalize_to_decimal,
    normalize_to_int,
    format_number_short,
    to_decimal_2,
    get_currency_name,
)


def test_normalize_to_int_with_nulls():
    """Prueba normalize_to_int con valores nulos y notación científica."""

    df = pd.DataFrame({
        'x': ['0.314e3', None, '5.23e5', '0', '7389', '2.1']
    })

    result = normalize_to_int(df.copy(), columns=['x'])

    expected = pd.DataFrame({
        'x': [314, None, 523000, 0, 7389, 2]
    })

    assert_frame_equal(result, expected)


def test_normalize_to_int_non_numeric_values():
    """Prueba que falle si hay valores no numéricos."""

    df = pd.DataFrame({'a': ['10', 'abc', '30']})

    with pytest.raises(ValueError):
        normalize_to_int(df, columns=['a'])


def test_normalize_to_decimal_basic():
    """Prueba normalize_to_decimal con números flotantes, enteros y nulos."""

    df = pd.DataFrame({
        'a': ['3.14256789', '2', None, '0.00052'],
        'b': ['1.535e-3', '2.5e-8', None, '30.987654321']
    })

    result = normalize_to_decimal(df.copy(), decimals=4, columns=['a', 'b'])

    expected = pd.DataFrame({
        'a': [
            Decimal('3.1426'),
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

    assert_frame_equal(result, expected)


def test_normalize_to_decimal_non_numeric_string():
    """Prueba que falle si se intenta convertir un valor no numérico."""

    df = pd.DataFrame({'x': ['abc', '123']})

    with pytest.raises(Exception):
        normalize_to_decimal(df.copy(), columns=['x'])


def test_format_number_short():
    """Verifica que la función formatee correctamente diferentes magnitudes."""

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


def test_to_decimal_2_valid_numbers():
    """Prueba to_decimal_2 con distintos tipos de números."""

    assert to_decimal_2(10) == Decimal("10.00")
    assert to_decimal_2(10.123) == Decimal("10.12")
    assert to_decimal_2("10.126") == Decimal("10.13")
    assert to_decimal_2(Decimal("3.456")) == Decimal("3.46")


def test_get_currency_name_valid_symbols():
    """Prueba get_currency_name con símbolos válidos."""

    assert get_currency_name("USD") == "US Dollar"
    assert get_currency_name("eur") == "Euro"
    assert get_currency_name(" ArS ") == "Argentine Peso"


def test_get_currency_name_invalid_symbol():
    """Prueba get_currency_name con símbolos inválidos."""

    assert get_currency_name("XYZ") == "Unknown currency"
    assert get_currency_name("") == "Unknown currency"
    assert get_currency_name("pesos") == "Unknown currency"
