#!/bin/bash

# Step 1: Create a raw database from CSV files
echo "\n.......Creating raw database......."
python3 src/database.py -de True -dn "inventory_data"

# Step 2: Upload raw data to the database after creating the table
echo "\n.......Creating table & uploading data......."
python3 src/database.py -dn "inventory_data" -t upload-to-database

# Step 3: Execute ETL process (Extract, Transform, Load)
echo "\n.......Running ETL......."
python3 src/etl.py

# Step 4: Create a processed version of the database
echo "\n.......Creating processed database......."
python3 src/database.py -de True -dn "processed_inventory_data"

# Step 5: Upload cleaned and processed data to the new database
echo "\n.......Creating table & uploading data......."
python3 src/database.py -dn "processed_inventory_data" -t cleaned-upload-to-database

# Step 6: Extract data from the database and upload it to S3 storage
echo "\n.......Extracting data & uploading to S3......."
python3 main.py -t data_extraction

# Step 7: Train ML models and upload predictions to Google Sheets
echo "\n.......Running modeling scripts & uploading predictions to gsheet......."
python3 src/prophet_modeling.py # Run Prophet model
python3 src/rf_modeling.py # Run Random Forest model
python3 src/xgb_modeling.py # Run XGBoost model