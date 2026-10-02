SELECT
    -- 8. Total Delivery Attempts
    COUNT(*) AS total_delivery_attempts,
    
    -- 9. First-Attempt Success Rate
    SUM(CASE WHEN is_first_attempt_flag = TRUE AND attempt_outcome = 'success' THEN 1 ELSE 0 END) * 1.0 / 
        NULLIF(COUNT(DISTINCT order_id), 0) AS first_attempt_success_rate,
    
    -- 10. Average Attempts per Order
    COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT order_id), 0) AS average_attempts_per_order,
    
    -- 11. Attempt Failure Rate
    SUM(CASE WHEN attempt_outcome = 'failed' THEN 1 ELSE 0 END) * 1.0 / 
        NULLIF(COUNT(*), 0) AS attempt_failure_rate

FROM marts.fact_delivery_attempts;