SELECT 
    w.weather_severity_bucket,
    z.dominant_state,
    COUNT(*) AS total_attempts,
    SUM(CASE WHEN a.attempt_outcome = 'failed' THEN 1 ELSE 0 END) * 1.0 / 
        NULLIF(COUNT(*), 0) AS attempt_failure_rate
FROM marts.fact_delivery_attempts a
JOIN marts.dim_weather w ON a.weather_key = w.weather_key
JOIN marts.dim_zone z ON a.zone_key = z.zone_key
GROUP BY w.weather_severity_bucket, z.dominant_state
ORDER BY attempt_failure_rate DESC;