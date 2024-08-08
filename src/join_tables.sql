SELECT * FROM sale_processed.sales_processed
LEFT JOIN sale_processed.sensor_processed 
ON sale_processed.sales_processed.product_id = sale_processed.sensor_processed.product_id;

SELECT * FROM sale_processed.sales_processed
LEFT JOIN sale_processed.temp_processed
ON sale_processed.sales_processed.timestamp = sale_processed.temp_processed.timestamp;