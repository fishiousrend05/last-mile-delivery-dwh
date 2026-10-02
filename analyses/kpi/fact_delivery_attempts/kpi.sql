SELECT
    -- 8. Total Delivery Attempts
    COUNT(*) AS total_delivery_attempts,
    
    -- 9. First-Attempt Success Rate
    -- Tận dụng luôn cờ is_first_attempt_flag đã có sẵn trong bảng
    SUM(CASE WHEN is_first_attempt_flag = TRUE AND attempt_outcome = 'delivered' THEN 1 ELSE 0 END) * 1.0 / 
        NULLIF(COUNT(DISTINCT order_id), 0) AS first_attempt_success_rate,
    
    -- 10. Average Attempts per Order
    COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT order_id), 0) AS average_attempts_per_order,
    
    -- 11. Attempt Failure Rate
    SUM(CASE WHEN attempt_outcome != 'delivered' THEN 1 ELSE 0 END) * 1.0 / 
        NULLIF(COUNT(*), 0) AS attempt_failure_rate
        
    -- KPI 12. Average SLA Risk Score được ẩn đi vì cột sla_risk_score chưa được implement
    -- AVG(sla_risk_score) AS average_sla_risk_score

FROM marts.fact_delivery_attempts;