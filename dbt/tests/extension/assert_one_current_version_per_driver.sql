-- dbt/tests/extension/assert_one_current_version_per_driver.sql
-- Mỗi dòng trả về = 1 driver_id có SỐ LƯỢNG is_current != 1 — tức 0 (driver
-- không có version nào đang hiệu lực) hoặc >1 (2 version cùng claim current,
-- gãy hẳn ngữ nghĩa SCD2). is_current=true phải là is_final_snapshot_version
-- (dbt_valid_to IS NULL), nên bất kỳ driver nào cũng phải có ĐÚNG 1 dòng
-- current tại mọi thời điểm sau khi build.

select
    driver_id,
    count(*) filter (where is_current) as n_current_versions
from {{ ref('dim_driver') }}
group by driver_id
having count(*) filter (where is_current) != 1