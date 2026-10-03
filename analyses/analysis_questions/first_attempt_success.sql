SELECT 
    z.zone_label,
    w.weather_severity_bucket,
    COUNT(DISTINCT a.order_id) AS total_orders,
    SUM(CASE WHEN a.is_first_attempt_flag = TRUE AND a.attempt_outcome = 'success' THEN 1 ELSE 0 END) * 1.0 / 
        NULLIF(COUNT(DISTINCT a.order_id), 0) AS first_attempt_success_rate
FROM marts.fact_delivery_attempts a
JOIN marts.dim_zone z ON a.zone_key = z.zone_key
JOIN marts.dim_weather w ON a.weather_key = w.weather_key
GROUP BY z.zone_label, w.weather_severity_bucket
HAVING COUNT(DISTINCT a.order_id) > 50
ORDER BY first_attempt_success_rate ASC -- Sắp xếp tăng dần để tìm các khu vực tệ nhất
LIMIT 10;