WITH base AS (
    SELECT f.*, TO_CHAR(d.full_date, 'YYYY-MM') AS order_month
    FROM marts.fact_order_lifecycle f
    LEFT JOIN marts.dim_date d ON f.purchase_date_key = d.date_key
    WHERE d.is_clean_window_flag = true
)

SELECT
    order_month,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(is_successful_flag::int) * 100.0 / NULLIF(SUM(is_eligible_flag::int), 0), 2) AS last_mile_completion_rate_pct,
    ROUND(
        SUM(CASE WHEN delivered_date_key IS NOT NULL AND estimated_delivery_breach_flag THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*) FILTER (WHERE delivered_date_key IS NOT NULL), 0)
    , 2) AS sla_breach_rate_pct,
    ROUND(AVG(last_mile_delivery_days) FILTER (WHERE delivered_date_key IS NOT NULL), 2) AS avg_delivery_lag_days,
    ROUND(AVG(estimated_delivery_delay_days) FILTER (WHERE delivered_date_key IS NOT NULL), 2) AS avg_sla_delay_days,
    ROUND(AVG(total_fulfillment_days) FILTER (WHERE delivered_date_key IS NOT NULL), 2) AS avg_fulfillment_time_days
FROM base
GROUP BY order_month
ORDER BY order_month;