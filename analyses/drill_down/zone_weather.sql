SELECT 
    w.weather_severity_bucket,
    COUNT(f.order_id) AS total_orders,
    SUM(CASE WHEN f.estimated_delivery_breach_flag = TRUE THEN 1 ELSE 0 END) AS total_breaches,
    SUM(CASE WHEN f.estimated_delivery_breach_flag = TRUE THEN 1 ELSE 0 END) * 1.0 / 
        NULLIF(COUNT(f.order_id), 0) AS breach_rate
FROM marts.fact_order_lifecycle f
JOIN marts.dim_weather w ON f.delivery_weather_key = w.weather_key
JOIN marts.dim_zone z ON f.customer_zone_key = z.zone_key
WHERE f.delivered_date_key IS NOT NULL
  AND z.zone_label = '<NHẬP_ZONE_LABEL>' -- Ví dụ: 'SP-0007'
GROUP BY w.weather_severity_bucket
ORDER BY 
    CASE WHEN w.weather_severity_bucket = 'High' THEN 1
         WHEN w.weather_severity_bucket = 'Medium' THEN 2
         WHEN w.weather_severity_bucket = 'Low' THEN 3
         ELSE 4 END;