import pandas as pd
from prophet import Prophet
from src.utils import authenticate_aws, upload_to_google_sheet


# Download file from s3 bucket
client = authenticate_aws(service='s3')
bucket_name = 'test-d2p-bucket'
object_name = 'inventory_data.csv'
download_file_path = 'data/inventory_data.csv'

client.download_file(bucket_name, object_name, download_file_path)

df = pd.read_csv(download_file_path)

df['timestamp'] = pd.to_datetime(df['timestamp'])

df1 = df.drop(['product_id','quantity','temperature'], axis=1)

df1 = df1.rename(columns={'estimated_stock_pct': 'y', 'timestamp': 'ds'})

m = Prophet()
m.fit(df1)

# Create future dates for forecasting
future = m.make_future_dataframe(periods=200, freq='h')

forecast = m.predict(future)

# Select only the 'ds' and 'yhat' columns from the forecast
forecast_full = forecast[['ds', 'yhat']]

# Append the forecast to the historical data
combined_df = pd.concat([df1, forecast_full], ignore_index=True)

upload_to_google_sheet(spreadsheet_id='1bN4obw3LQD1ZlYoTBuB3bs1AqwzXK1zdaUxWZRz8jJI',
                       df=combined_df,
                       worksheet_name='prophet')