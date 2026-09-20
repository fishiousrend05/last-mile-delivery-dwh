WITH base AS (
    SELECT f.*, z.zone_id, z.dominant_state
    FROM marts.fact_order_lifecycle f
    LEFT JOIN marts.dim_date d ON f.purchase_date_key = d.date_key
    LEFT JOIN marts.dim_zone z ON f.customer_zone_key = z.zone_key
    WHERE d.is_clean_window_flag = true
)

SELECT
    zone_id,
    dominant_state,
    COUNT(*) FILTER (WHERE delivered_date_key IS NOT NULL) AS delivered_orders,
    ROUND(
        SUM(CASE WHEN delivered_date_key IS NOT NULL AND estimated_delivery_breach_flag THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*) FILTER (WHERE delivered_date_key IS NOT NULL), 0)
    , 2) AS sla_breach_rate_pct,
    ROUND(AVG(last_mile_delivery_days) FILTER (WHERE delivered_date_key IS NOT NULL), 2) AS avg_delivery_lag_days
FROM base
GROUP BY zone_id, dominant_state
HAVING COUNT(*) FILTER (WHERE delivered_date_key IS NOT NULL) >= 30  -- loại zone quá ít đơn, tránh tỷ lệ nhiễu do mẫu nhỏ
ORDER BY sla_breach_rate_pct DESC
LIMIT 20;