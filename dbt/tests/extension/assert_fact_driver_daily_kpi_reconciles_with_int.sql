-- dbt/tests/extension/assert_fact_driver_daily_kpi_reconciles_with_int.sql
-- Reconciliation mart <-> intermediate, cùng pattern đã dùng cho
-- fact_delivery_attempts — đảm bảo 2 LEFT JOIN resolve key không fan-out
-- hay mất dòng.

with fact as (
    select
        count(*) as n_rows,
        sum(total_attempts) as total_attempts,
        sum(successful_attempts) as successful_attempts
    from {{ ref('fact_driver_daily_kpi') }}
),

source as (
    select
        count(*) as n_rows,
        sum(total_attempts) as total_attempts,
        sum(successful_attempts) as successful_attempts
    from {{ ref('int_driver_daily') }}
)

select fact.*, source.*
from fact
cross join source
where fact.n_rows != source.n_rows
   or fact.total_attempts != source.total_attempts
   or fact.successful_attempts != source.successful_attempts