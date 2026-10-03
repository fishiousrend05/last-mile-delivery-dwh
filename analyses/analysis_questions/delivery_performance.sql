SELECT 
    z.dominant_state,
    d.year,
    d.quarter,
    COUNT(f.order_id) AS total_delivered_orders,
    SUM(CASE WHEN f.estimated_delivery_breach_flag = TRUE THEN 1 ELSE 0 END) * 1.0 / 
        NULLIF(COUNT(f.order_id), 0) AS estimated_delivery_breach_rate
FROM marts.fact_order_lifecycle f
JOIN marts.dim_zone z ON f.customer_zone_key = z.zone_key
JOIN marts.dim_date d ON f.delivered_date_key = d.date_key
WHERE f.delivered_date_key IS NOT NULL 
  AND d.is_clean_window_flag = TRUE -- Giới hạn trong khoảng baseline
GROUP BY z.dominant_state, d.year, d.quarter
HAVING COUNT(f.order_id) > 100 -- Lọc nhiễu các khu vực quá ít đơn
ORDER BY estimated_delivery_breach_rate DESC
LIMIT 10;