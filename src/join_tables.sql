SELECT 
    a.timestamp,
    a.product_id,
    a.estimated_stock_pct,
    b.quantity,
    c.temperature
FROM sale_processed.sensor_processed a
LEFT JOIN sale_processed.sales_processed b
    ON a.product_id = b.product_id AND a.timestamp = b.timestamp
LEFT JOIN sale_processed.temp_processed c
    ON a.timestamp = c.timestamp;