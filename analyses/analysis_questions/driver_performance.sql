SELECT 
    d.driver_id,
    SUM(k.total_attempts) AS total_workload_attempts,
    SUM(k.successful_attempts) * 1.0 / NULLIF(SUM(k.total_attempts), 0) AS overall_success_rate,
    -- Trung bình số khu vực tài xế phải chạy mỗi ngày hoạt động
    SUM(k.zones_covered_count) * 1.0 / NULLIF(COUNT(CASE WHEN k.is_active_day = TRUE THEN 1 END), 0) AS avg_zones_per_active_day
FROM marts.fact_driver_daily_kpi k
JOIN marts.dim_driver d ON k.driver_key = d.driver_key
GROUP BY d.driver_id
HAVING SUM(k.total_attempts) > 100 -- Chỉ xét các tài xế có số lần chạy đáng kể
ORDER BY overall_success_rate ASC
LIMIT 20;