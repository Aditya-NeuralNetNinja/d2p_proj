import pandas as pd
import numpy as np
import xgboost as xgb
from utils import read_file_of_s3, upload_to_google_sheet

bucket_name = 'test-d2p-bucket'
object_name = 'inventory_data.csv'

# Category grouping dictionary
category_grouping = {
    'dairy': 'Dairy Products',
    'cheese': 'Dairy Products',
    'baking': 'Dairy Products',
    'baked goods': 'Packaged Goods',
    'fruit': 'Fresh Produce',
    'vegetables': 'Fresh Produce',
    'packaged foods': 'Packaged Goods',
    'canned foods': 'Packaged Goods',
    'snacks': 'Packaged Goods',
    'frozen': 'Frozen and Refrigerated',
    'refrigerated items': 'Frozen and Refrigerated',
    'meat': 'Frozen and Refrigerated',
    'seafood': 'Frozen and Refrigerated',
    'condiments and sauces': 'Condiments and Spices',
    'spices and herbs': 'Condiments and Spices',
    'cleaning products': 'Household and Personal Care',
    'personal care': 'Household and Personal Care',
    'medicine': 'Household and Personal Care',
    'kitchen': 'Household and Personal Care',
    'beverages': 'Packaged Goods',
    'baby products': 'Packaged Goods',
    'pets': 'Household and Personal Care'
}

def feature_engineering(df: pd.DataFrame, lags: list) -> pd.DataFrame:
    """
    Performs feature engineering on the input dataframe by generating lag features,
    filling missing values, and creating categorical and time-based features.

    Args:
        df (pd.DataFrame): Input dataframe containing historical data.
        lags (list): List of time lags to generate lagged features for the target column.

    Returns:
        pd.DataFrame: DataFrame with lagged features, dummies for categories, and time-based features.
    """
    
    # Fill missing values in 'quantity' column
    df['quantity'] = df['quantity'].fillna(df['quantity'].median())
    
    # Create lagged features
    for lag in lags:
        df[f'estimated_stock_pct_lag_{lag}'] = df['estimated_stock_pct'].shift(lag)
        
    # Map categories to grouped categories
    df['grouped_category'] = df['category'].replace(category_grouping)

    # Create dummy variables for grouped categories
    category_dummies = pd.get_dummies(df['grouped_category'], prefix='category')
    df = pd.concat([df, category_dummies], axis=1)
    
    # Time-based features
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    
    # Forward fill missing values and then backward fill any remaining missing values
    df = df.ffill().bfill()
    
    return df

def split_data(df: pd.DataFrame, target: str) -> tuple:
    """
    Splits the data into training and testing sets based on a specific date.

    Args:
        df (pd.DataFrame): Input dataframe.
        target (str): Target column for prediction.

    Returns:
        tuple: Returns X_train, X_test, y_train, y_test dataframes.
    """

    # Split data into train and test sets
    train_data = df[df['timestamp'] < '2022-03-06']
    test_data = df[df['timestamp'] >= '2022-03-06']

    # Split target variable
    y_train = train_data[target]
    y_test = test_data[target]

    # Drop unnecessary columns from train and test dataframes
    X_train = train_data.drop([target, 'product_id', 'timestamp', 'category', 'grouped_category'], axis=1)
    X_test = test_data.drop([target, 'product_id', 'timestamp', 'category', 'grouped_category'], axis=1)

    return X_train, X_test, y_train, y_test

def build_xgboost_model(X_train: pd.DataFrame, y_train: pd.Series) -> xgb.XGBRegressor:
    """
    Builds and trains an XGBoost regression model on the training data.

    Args:
        X_train (pd.DataFrame): Training feature data.
        y_train (pd.Series): Training target data.

    Returns:
        xgb.XGBRegressor: Trained XGBoost model.
    """

    # Build and train XGBoost model
    model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=100, learning_rate=0.05, max_depth=6)
    model.fit(X_train, y_train)

    return model

def forecast_next_3_months(df: pd.DataFrame, model: xgb.XGBRegressor) -> pd.DataFrame:
    """
    Generates feature data for future timestamps and makes predictions using the trained XGBoost model.

    Args:
        df (pd.DataFrame): Historical dataframe for reference.
        model (xgb.XGBRegressor): Trained XGBoost model.

    Returns:
        pd.DataFrame: Dataframe containing future timestamps and predicted values.
    """
    
    # Ensure timestamp is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Generate feature data for future timestamps
    future_dates = pd.date_range(start=df['timestamp'].max(), periods=90, freq='h')

    product_info = df[['product_id', 'category']].drop_duplicates().set_index('product_id')
    
    future_data = pd.DataFrame([(date, pid) for date in future_dates for pid in product_info.index],
                               columns=['timestamp', 'product_id'])

    # Add category information
    future_data['category'] = future_data['product_id'].map(product_info['category'])

    # For each product, use the most recent unit price
    last_prices = df.groupby('product_id')['unit_price'].last()
    future_data['unit_price'] = future_data['product_id'].map(last_prices)
    
    # Use the average historical quantity sold for each product
    avg_quantities = df.groupby('product_id')['quantity'].mean()
    future_data['quantity'] = future_data['product_id'].map(avg_quantities)
    
    # For temperature, use the average temperature for each hour of the day and month
    temp_avg = df.groupby([df['timestamp'].dt.month, df['timestamp'].dt.hour])['temperature'].mean()
    overall_mean_temp = df['temperature'].mean()

    def get_temperature(row):
        try:
            return temp_avg.loc[row['timestamp'].month, row['timestamp'].hour]
        except KeyError:
            return overall_mean_temp

    future_data['temperature'] = future_data.apply(get_temperature, axis=1)
    
    # Create lag features for estimated stock percentage
    for lag in [1, 2, 3, 6, 12, 24]:
        col_name = f'estimated_stock_pct_lag_{lag}'

        # Compute the lagged features for the original dataframe if not present
        if col_name not in df.columns:
            df[col_name] = df.groupby('product_id')['estimated_stock_pct'].shift(lag)

        last_stock = df.groupby('product_id')[col_name].last()
        future_data[col_name] = future_data['product_id'].map(last_stock)

    # Add grouped category
    future_data['grouped_category'] = future_data['category'].replace(category_grouping)

    category_dummies_future = pd.get_dummies(future_data['grouped_category'], prefix='category')

    future_data = pd.concat([future_data, category_dummies_future], axis=1)

    bool_columns = ['category_Condiments and Spices', 'category_Dairy Products', 'category_Fresh Produce',
                'category_Frozen and Refrigerated', 'category_Household and Personal Care', 'category_Packaged Goods']

    future_data[bool_columns] = future_data[bool_columns].astype(int)

    # Time-based features
    future_data['timestamp'] = pd.to_datetime(future_data['timestamp'])
    future_data['month'] = future_data['timestamp'].dt.month
    future_data['day'] = future_data['timestamp'].dt.day
    future_data['hour'] = future_data['timestamp'].dt.hour
    future_data['day_of_week'] = future_data['timestamp'].dt.dayofweek
    future_data['is_weekend'] = (future_data['day_of_week'] >= 5).astype(int)
    
    # Forward fill missing values
    future_data = future_data.ffill()
    
    # Drop unnecessary columns
    X_future = future_data.drop(['timestamp', 'product_id', 'category', 'grouped_category'], axis=1)

    # Make predictions using trained model
    future_data['estimated_stock_pct'] = model.predict(X_future)

    return future_data

def process(df: pd.DataFrame, target: str, lags: list) -> pd.DataFrame:
    """
    Orchestrates the complete process: feature engineering, data splitting, model training,
    forecasting, and merging of historical and forecasted data.

    Args:
        df (pd.DataFrame): Input dataframe.
        target (str): Target column for prediction.
        lags (list): List of lag features to create for the target column.

    Returns:
        pd.DataFrame: Merged dataframe with historical and forecasted predictions.
    """

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Perform feature engineering
    df_lagged = feature_engineering(df, lags)

    # Split data into train and test sets
    X_train, X_test, y_train, y_test = split_data(df_lagged, target)

    # Build and train XGBoost model
    model = build_xgboost_model(X_train, y_train)

    # Forecast next 3 months
    future_predictions = forecast_next_3_months(df, model)

    # Merge historical and forecasted dataframes
    merged_df = pd.concat([df_lagged, future_predictions], axis=0).reset_index(drop=True)
    
    # Create a new column 'is_forecast' to indicate whether the value of estimated_stock_pct is from historical or predicted data
    mask_historical = merged_df.index < len(df_lagged)
    merged_df['is_forecast'] = np.where(mask_historical, 0, 1)
    
    # Reorder columns to move 'is_forecast' to the immediate right of 'estimated_stock_pct'
    columns = merged_df.columns.tolist()
    columns.insert(columns.index('estimated_stock_pct') + 1, columns.pop(columns.index('is_forecast')))
    merged_df = merged_df.reindex(columns=columns)
    
    return merged_df

if __name__ == "__main__":
    df = read_file_of_s3(bucket_name=bucket_name, filename=object_name)
    target = 'estimated_stock_pct'
    lags = [1, 2, 3, 6, 12, 24]
    integrated_df = process(df, target, lags)

    # Save inegrated (historical & predictions) data file locally
    integrated_df.to_csv('data/predictions/xgb_predictions.csv', index=False)
    
    # Upload integrated dataframe to google sheet
    upload_to_google_sheet(spreadsheet_id='1nVyQhLnWrwvlROG8wRAbOtJPl9-ASiPAh9k_J1Mvi4I',
                           df=integrated_df,
                           worksheet_name='xgb')
    
    print('XGBoost modeling script executed successfully!')