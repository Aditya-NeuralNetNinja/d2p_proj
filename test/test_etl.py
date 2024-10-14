import pytest
import pandas as pd
from src.etl import convert_timestamp_to_hourly, aggregate


@pytest.fixture
def input_file():
    """
    Pytest fixture that returns the path to the input CSV file.

    Returns:
        str: Path to the input CSV file.
    """
    return 'data/processed/inventory_data.csv'  
    

def test_convert_timestamp_to_hourly_invalid_format(input_file):
    """
    Test that the function handles invalid timestamp format by checking the output.
    """
    df = pd.read_csv(input_file)
    
    # Create a sample dataframe with invalid timestamp format
    df.loc[0, 'timestamp'] = '2022-01-01 12:00:00'
    df.loc[1, 'timestamp'] = '2022/1/01 12:00:00'
    
    # Test the function and pass the 'timestamp' column explicitly
    result_df = convert_timestamp_to_hourly(df, column='timestamp')
    
    # Assert that the invalid timestamp is converted to NaT (Not a Timestamp)
    assert pd.isna(result_df.loc[1, 'timestamp']), "Invalid timestamp format should be coerced to NaT"