import pandas as pd
from prophet import Prophet

from src.utils import read_file_of_s3, upload_to_google_sheet

bucket_name = 'test-d2p-bucket'
object_name = 'inventory_data.csv'

def model(df: pd.DataFrame, periods: int) -> pd.DataFrame:
    """
    Train prophet model and forecast on future timestamps

    Args:
        df (pd.DataFrame): Input dataframe containing historical data
        periods (int): Number of timestamps of future that needs to be created

    Returns:
        pd.DataFrame: Resultant dataframe comprising of predictions on historical, futuristic data
    """
    # Initialize and train the Prophet model on historical data
    m = Prophet()
    m.fit(df)
    
    # Create future dates for forecasting, including historical dates
    future = m.make_future_dataframe(periods=periods, freq='h', include_history=True)
    
    # Forecast for both historical and future dates
    forecast = m.predict(future)
    return forecast

def process()->bool:
    """
    Preprocess input data, generate predictions, upload resultant DataFrame to Google Sheet

    Returns:
        bool: True, if function write the resultant DataFrame to Google Sheet, lest False
    """
    # Read data from S3
    df = read_file_of_s3(bucket_name=bucket_name, filename=object_name)

    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Drop unnecessary columns
    df = df.drop(['product_id', 'quantity', 'temperature'], axis=1)

    # Rename columns for Prophet
    df = df.rename(columns={'estimated_stock_pct': 'y', 'timestamp': 'ds'})

    # Generate forecast for both historical and future periods
    forecast = model(df, periods=200)

    # Merge the original 'y' values back with the forecasted 'yhat'
    forecast_merged = pd.merge(df[['ds', 'y']], forecast[['ds', 'yhat']], on='ds', how='right')

    # Upload the resultant DataFrame to Google Sheet
    upload_to_google_sheet(spreadsheet_id='1nVyQhLnWrwvlROG8wRAbOtJPl9-ASiPAh9k_J1Mvi4I',
                           df=forecast_merged,
                           worksheet_name='prophet')

# Run the process
process()