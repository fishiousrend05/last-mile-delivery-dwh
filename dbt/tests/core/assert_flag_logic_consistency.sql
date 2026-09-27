select order_id
from {{ ref('fact_order_lifecycle') }}
where (is_successful_flag and not is_eligible_flag)
   or (is_successful_flag and is_unresolved_outcome_flag)