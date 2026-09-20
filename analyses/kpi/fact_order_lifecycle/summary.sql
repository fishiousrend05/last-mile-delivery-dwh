WITH base AS (
    SELECT f.*
    FROM marts.fact_order_lifecycle f
    LEFT JOIN marts.dim_date d ON f.purchase_date_key = d.date_key
    WHERE d.is_clean_window_flag = true
)

SELECT
    -- KPI 1: Total Orders
    COUNT(DISTINCT order_id) AS total_orders,

    -- KPI 2: Last-Mile Completion Rate
    ROUND(
        SUM(is_successful_flag::int) * 100.0
        / NULLIF(SUM(is_eligible_flag::int), 0)
    , 2) AS last_mile_completion_rate_pct,

    -- KPI 3: Estimated Delivery Breach Rate (trên đơn đã giao tới khách)
    ROUND(
        SUM(CASE WHEN delivered_date_key IS NOT NULL AND estimated_delivery_breach_flag THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*) FILTER (WHERE delivered_date_key IS NOT NULL), 0)
    , 2) AS estimated_delivery_breach_rate_pct,

    -- KPI 4: Average Delivery Lag (chặng last-mile: carrier -> customer)
    ROUND(AVG(last_mile_delivery_days) FILTER (WHERE delivered_date_key IS NOT NULL), 2) AS avg_delivery_lag_days,

    -- KPI 5: Average Estimated Delivery Delay
    ROUND(AVG(estimated_delivery_delay_days) FILTER (WHERE delivered_date_key IS NOT NULL), 2) AS avg_estimated_delivery_delay_days,

    -- KPI 6: Average Fulfillment Time (toàn vòng đời: purchase -> customer)
    ROUND(AVG(total_fulfillment_days) FILTER (WHERE delivered_date_key IS NOT NULL), 2) AS avg_fulfillment_time_days,

    -- KPI 7: Cancellation / Unavailable Rate
    ROUND(
        SUM(CASE WHEN order_status IN ('canceled', 'unavailable') THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0)
    , 2) AS cancellation_unavailable_rate_pct

FROM base;