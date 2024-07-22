import pandas as pd
from datetime import datetime

def read(file_path:str)-> pd.DataFrame:    
    return pd.read_csv(file_path)

df1 = read('data/sales.csv')
df2 = read('data/sensor_stock_levels.csv')
df3 = read('data/sensor_storage_temperature.csv')

# Convert 'timestamp' column to datetime
df1['timestamp'] = pd.to_datetime(df1['timestamp'], format='%Y-%m-%d %H:%M:%S')
df2['timestamp'] = pd.to_datetime(df2['timestamp'], format='%Y-%m-%d %H:%M:%S')
df3['timestamp'] = pd.to_datetime(df3['timestamp'], format='%Y-%m-%d %H:%M:%S')

# Convert timestamp to hourly level
def convert_timestamp_to_hourly(data: pd.DataFrame = None, column: str = None) -> pd.DataFrame:
    """
    Convert timestamp to hourly level

    Args:
        data (pd.DataFrame, optional): Input dataframe. Defaults to None.
        column (str, optional): Column related to datetime data. Defaults to None.

    Returns:
        DataFrame: resultant dataframe
    """
    dummy = data.copy()
    new_ts = dummy[column].tolist()
    new_ts = [i.strftime('%Y-%m-%d %H:00:00') for i in new_ts]
    new_ts = [datetime.strptime(i, '%Y-%m-%d %H:00:00') for i in new_ts]
    dummy[column] = new_ts
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
