-- Phải LUÔN = 0 dòng ở bất kỳ bản build nào dùng để báo cáo/nộp bài.
-- Nếu test này FAIL, nghĩa là bản fact_order_lifecycle hiện tại đã bị build
-- với demo_cutoff_date còn sót lại -> KHÔNG được dùng bản này cho báo cáo.
select count(*) as leaked_masked_rows
from {{ ref('fact_order_lifecycle') }}
where is_demo_masked_flag = true
having count(*) > 0