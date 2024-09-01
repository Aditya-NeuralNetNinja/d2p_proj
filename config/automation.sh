#!/bin/bash

# load data from csv files into a database
echo "Creating raw database......."
python3 src/database.py -de True -dn "inventory_data"

# normalize and clean data, and upload to database
echo "Creating table & uploading data......."
python3 src/database.py -dn "inventory_data" -t upload-to-database

echo "Running ETL......."
python3 src/etl.py

echo "Creating processed database......."
python3 src/database.py -de True -dn "processed_inventory_data"

echo "Creating table & uploading data......."
python3 src/database.py -dn "processed_inventory_data" -t cleaned-upload-to-database

# train and evaluate machine learning models
echo "Extracting data & uploading to S3......."
python3 main.py -t data_extraction

echo "Running modelling & uploading to gsheet......."
python3 main.py -t modeling