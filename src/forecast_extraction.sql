SELECT 
    a.timestamp,
    a.product_id,
    a.estimated_stock_pct,
    b.quantity,
    c.temperature
FROM inventory_processed_db.stock_processed a
LEFT JOIN inventory_processed_db.sales_processed b
    ON a.product_id = b.product_id AND a.timestamp = b.timestamp
LEFT JOIN inventory_processed_db.temp_processed c
    ON a.timestamp = c.timestamp;