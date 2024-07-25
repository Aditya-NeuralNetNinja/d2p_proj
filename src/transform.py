# Imports
import pandas as pd
from utils import get_data

# Build pandas dataframe with 'timestamp' column datatype change
df1 = get_data('data/sales.csv')
df2 = get_data('data/sensor_stock_levels.csv')
df3 = get_data('data/sensor_storage_temperature.csv')

# Convert timestamp to hourly level
def convert_timestamp_to_hourly(df:pd.DataFrame = None, column:str = None) -> pd.DataFrame:
    """
    Convert timestamp to hourly level

    Args:
        df (pd.DataFrame, optional): Input dataframe. Defaults to None.
        column (str, optional): Column related to datetime data. Defaults to None.

    Returns:
        DataFrame: resultant dataframe with hourly timestamps.
    """
    dummy = df.copy()
    dummy[column] = pd.to_datetime(dummy[column], format='%Y-%m-%d %H:%M:%S') # String to datetime datatype conversion
    dummy[column] = dummy[column].dt.floor('H') # Truncate timestamps to beginning of hour
    return dummy

df1_hourly = convert_timestamp_to_hourly(df1,'timestamp')
df2_hourly = convert_timestamp_to_hourly(df2,'timestamp')
df3_hourly = convert_timestamp_to_hourly(df3,'timestamp')

# Create separate dataframe + aggregation
sales_df = df1_hourly.groupby(by=['timestamp','product_id']).agg({'quantity' : 'sum'}).reset_index()
sensor_df = df2_hourly.groupby(by=['timestamp','product_id']).agg({'estimated_stock_pct' : 'mean'}).reset_index()
temp_df = df3_hourly.groupby(by=['timestamp']).agg({'temperature' : 'mean'}).reset_index()

# Export processed files to CSV format
sales_df.to_csv('sales_processed.csv', index=False)
sensor_df.to_csv('sensor_processed.csv', index=False)
temp_df.to_csv('temp_processed.csv', index=False)
