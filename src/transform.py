# Imports
import pandas as pd
from utils import get_data


# Build pandas dataframe with 'timestamp' column datatype change
df1 = get_data('data/sales.csv')
df2 = get_data('data/sensor_stock_levels.csv')
df3 = get_data('data/sensor_storage_temperature.csv')


# Convert timestamp to hourly level
def convert_timestamp_to_hourly(df: pd.DataFrame = None, column: str = None) -> pd.DataFrame:
    """
    Convert timestamp to hourly level

    Args:
        df (pd.DataFrame, optional): Input dataframe. Defaults to None.
        column (str, optional): Column related to datetime data. Defaults to None.

    Returns:
        DataFrame: resultant dataframe with hourly timestamps.
    """
    dummy = df.copy()
    dummy[column] = pd.to_datetime(dummy[column], format='%Y-%m-%d %H:%M:%S')  # String to datetime datatype conversion
    dummy[column] = dummy[column].dt.floor('h')  # Truncate timestamps to beginning of hour
    return dummy


# Create separate dataframe + aggregation
# Create separate dataframe + aggregation
def aggregate(df: pd.DataFrame, col1: str, aggregate_col: str, operation: str, col2: str = None) -> pd.DataFrame:
    """
    Performs a groupby and aggregation operation on a pandas DataFrame.

    Groups the DataFrame by specified columns and applies the given operation
    to the aggregate column.

    Args:
        df (pd.DataFrame): The input pandas DataFrame.
        col1 (str): The first column to group by.
        aggregate_col (str): The column to aggregate.
        operation (str): The aggregation operation to apply (e.g., 'sum', 'mean', 'min', 'max').
        col2 (str, optional): The second column to group by. Defaults to None.

    Returns:
        pd.DataFrame: A new DataFrame containing the grouped and aggregated data.
    """
    group_cols = [col1] if col2 is None else [col1, col2]
    df_new = df.groupby(by=group_cols).agg({aggregate_col: operation}).reset_index()
    return df_new



# Using convert_timestamp_to_hourly function
df1_hourly = convert_timestamp_to_hourly(df1, 'timestamp')
df2_hourly = convert_timestamp_to_hourly(df2, 'timestamp')
df3_hourly = convert_timestamp_to_hourly(df3, 'timestamp')

# Using the aggregate function
# Using the aggregate function
sales_df = aggregate(df1_hourly, 'timestamp', 'quantity', 'sum', 'product_id')
sensor_df = aggregate(df2_hourly, 'timestamp', 'estimated_stock_pct', 'mean', 'product_id')
temp_df = aggregate(df3_hourly, 'timestamp', 'temperature', 'mean')  # No second column, do not pass 'col2'

# Export processed files to CSV format
sales_df.to_csv('data/sales_processed.csv', index=False)
sensor_df.to_csv('data/stock_processed.csv', index=False)
temp_df.to_csv('data/temp_processed.csv', index=False)