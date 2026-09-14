with source as (
    select * from {{ source('landing_olist', 'olist_order_reviews') }}
),
renamed as (
    select
        review_id,
        order_id,
        review_score::int as review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date::timestamptz as review_creation_date,
        review_answer_timestamp::timestamptz as review_answer_timestamp
    from source
)
select * from renamed