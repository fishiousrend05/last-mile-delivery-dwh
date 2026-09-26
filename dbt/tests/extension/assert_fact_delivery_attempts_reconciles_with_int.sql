-- dbt/tests/extension/assert_fact_delivery_attempts_reconciles_with_int.sql
-- Reconciliation giữa mart và intermediate — đảm bảo các LEFT JOIN resolve
-- key ở fact không vô tình fan-out (nhân dòng) hoặc mất dòng (nếu một JOIN
-- lẽ ra phải INNER lại viết nhầm thành filter INNER). Cùng cách bạn đã làm
-- ở Phase 3/4 core (reconciliation staging <-> intermediate).

with fact as (
    select count(*) as n_rows, count(distinct attempt_id) as n_distinct
    from {{ ref('fact_delivery_attempts') }}
),

source as (
    select count(*) as n_rows
    from {{ ref('int_delivery_attempts') }}
)

select
    fact.n_rows as fact_row_count,
    fact.n_distinct as fact_distinct_attempt_id,
    source.n_rows as source_row_count
from fact
cross join source
where fact.n_rows != source.n_rows
   or fact.n_distinct != fact.n_rows