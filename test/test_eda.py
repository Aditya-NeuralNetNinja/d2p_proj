import pytest
import pandas as pd
from src.utils import get_data  

@pytest.fixture
def input_file():
    """
    Pytest fixture that returns the path to the input CSV file.

    Returns:
        str: Path to the input CSV file.
    """
    return 'data/inventory_data.csv'  

@pytest.fixture
def expected_columns():
    """
    Pytest fixture that returns a dictionary of expected column names and their data types.

    Returns:
        dict: A dictionary where keys are column names and values are expected data types.
    """
    return {
        "timestamp": "object",
        "product_id": "object",
        "estimated_stock_pct": "float64",
        "category": "object",
        "unit_price": "float64",
        "quantity": "float64",
        "temperature": "float64"
    }


def test_get_data_column_types(input_file: str, expected_columns: dict[str, str])->None:
    """
    Test if the data types of columns in the DataFrame match the expected types.

    Args:
        input_file (str): Path to the input CSV file.
        expected_columns (dict): Dictionary of expected column names and their data types.

    Raises:
        AssertionError: If a column is missing or its data type doesn't match the expected type.
    """
    df = get_data(input_file)
    for column_name, expected_dtype in expected_columns.items():
        assert column_name in df.columns, f"Column '{column_name}' not found in the DataFrame"
        assert df[column_name].dtype == expected_dtype, f"Expected {column_name} to be {expected_dtype}, but got {df[column_name].dtype}"


def test_len_data_columns(input_file: str, expected_columns: dict[str, str])-> None:
    """
    Check if the number of columns in the input dataset matches the expected count.

    Args:
        input_file (str): Path to the input CSV file.
        expected_columns (dict): Dictionary of expected column names and their data types.

    Raises:
        AssertionError: If the number of columns doesn't match the expected count.
    """
    df = get_data(input_file)
    expected_column_count = len(expected_columns)
    assert len(df.columns) == expected_column_count, f"Expected {expected_column_count} columns, but got {len(df.columns)}"


def test_get_data_no_unnamed_column(input_file: str)->None:
    """
    Test if the DataFrame does not contain an 'Unnamed: 0' column.

    Args:
        input_file (str): Path to the input CSV file.

    Raises:
        AssertionError: If the DataFrame contains an 'Unnamed: 0' column.
    """
    df = get_data(input_file)
    assert 'Unnamed: 0' not in df.columns, "DataFrame should not contain 'Unnamed: 0' column"


def test_get_data_non_null_values(input_file: str, expected_columns: dict[str, str])->None:
    """
    Test if each column in the DataFrame contains at least one non-null value.

    Args:
        input_file (str): Path to the input CSV file.
        expected_columns (dict): Dictionary of expected column names and their data types.

    Raises:
        AssertionError: If any column contains only null values.
    """
    df = get_data(input_file)
    for column_name in expected_columns.keys():
        has_non_null = df[column_name].notnull().any()
        assert has_non_null, f"Expected {column_name} to have non-null values"