SELECT
    -- 13. Driver Success Rate
    -- Tính tổng số lần giao thành công chia cho tổng số nỗ lực giao hàng trên toàn hệ thống
    SUM(successful_attempts) * 1.0 / NULLIF(SUM(total_attempts), 0) AS driver_success_rate,
    
    -- 14. Active Driver Rate
    -- Tính tỷ lệ tài xế có hoạt động (is_active_day = TRUE) trên tổng số tài xế được ghi nhận
    COUNT(DISTINCT CASE WHEN is_active_day = TRUE THEN driver_key END) * 1.0 / 
        NULLIF(COUNT(DISTINCT driver_key), 0) AS active_driver_rate

FROM marts.fact_driver_daily_kpi;