SELECT 
    z.zone_label,
    z.dominant_state,
    COUNT(f.order_id) AS total_orders,
    SUM(CASE WHEN f.estimated_delivery_breach_flag = TRUE THEN 1 ELSE 0 END) AS total_breaches,
    SUM(CASE WHEN f.estimated_delivery_breach_flag = TRUE THEN 1 ELSE 0 END) * 1.0 / 
        NULLIF(COUNT(f.order_id), 0) AS breach_rate
FROM marts.fact_order_lifecycle f
JOIN marts.dim_zone z ON f.customer_zone_key = z.zone_key
WHERE f.delivered_date_key IS NOT NULL
GROUP BY z.zone_label, z.dominant_state
HAVING COUNT(f.order_id) > 50 -- Lọc các zone có đủ dữ liệu thống kê
ORDER BY total_breaches DESC, breach_rate DESC
LIMIT 20;