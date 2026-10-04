SELECT 
    d.full_date AS attempt_date,
    z.zone_label,
    w.weather_severity_bucket,
    COUNT(a.attempt_id) AS total_attempts,
    SUM(CASE WHEN a.attempt_outcome = 'failed' THEN 1 ELSE 0 END) AS failed_attempts,
    SUM(CASE WHEN a.attempt_outcome = 'failed' THEN 1 ELSE 0 END) * 1.0 / 
        NULLIF(COUNT(a.attempt_id), 0) AS failure_rate
FROM marts.fact_delivery_attempts a
JOIN marts.dim_date d ON a.attempt_date_key = d.date_key
JOIN marts.dim_zone z ON a.zone_key = z.zone_key
JOIN marts.dim_weather w ON a.weather_key = w.weather_key
JOIN marts.dim_driver dr ON a.driver_key = dr.driver_key
WHERE dr.driver_id = 'DRV0126' -- Khóa ngoại tài xế cần phân tích
GROUP BY d.full_date, z.zone_label, w.weather_severity_bucket
ORDER BY d.full_date DESC;

