-- dbt/models/marts/extension/dim_failed_reason.sql

{{ config(tags=['extension']) }}

/*
    Grain: 1 dòng/lý do thất bại — catalog tĩnh (9 lý do FR01-FR09), không
    versioned, không SCD (khác dim_driver). Nguồn: dbt/seeds/failed_reasons.csv
    qua stg_synthetic__failed_reasons — staging đã rename đúng key của
    FAILED_REASONS trong synthetic_generator.py, mart chỉ resolve key.
*/

select

    {{ dbt_utils.generate_surrogate_key(['failed_reason_id']) }} as failed_reason_key,

    failed_reason_id,
    failed_reason_category,
    failed_reason_name as failed_reason_desc

from {{ ref('stg_synthetic__failed_reasons') }}