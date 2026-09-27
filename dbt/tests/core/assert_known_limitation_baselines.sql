with counts as (
    select
        count(*) filter (where is_unresolved_outcome_flag) as orphan_shipped_count,
        count(*) filter (where order_status = 'delivered' and not is_successful_flag) as delivered_anomaly_count,
        count(*) filter (where last_mile_duration_quality = 'negative_duration') as negative_duration_count
    from {{ ref('fact_order_lifecycle') }}
)
select *
from counts
where orphan_shipped_count <> 1097
   or delivered_anomaly_count <> 9
   or negative_duration_count <> 23