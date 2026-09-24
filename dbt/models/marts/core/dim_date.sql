{{ config(
    materialized='table',
    indexes=[
        {'columns': ['full_date'], 'type': 'btree'}
    ]
) }}

with date_spine as (
    select generate_series(
        '2016-01-01'::date,
        '2018-12-31'::date,
        interval '1 day'
    )::date as full_date
),

holidays as (
    select * from {{ ref('stg_external__holidays') }}
),

commercial_events as (
    select * from {{ ref('stg_external__commercial_events') }}
),

enriched as (
    select
        ds.full_date,
        extract(dow from ds.full_date)::int as day_of_week,
        extract(month from ds.full_date)::int as month,
        extract(quarter from ds.full_date)::int as quarter,
        extract(year from ds.full_date)::int as year,
        (extract(dow from ds.full_date) in (0, 6)) as is_weekend,
        (h.holiday_date is not null) as is_holiday,
        h.holiday_name,
        coalesce(h.holiday_impact_tier, 'Tier_3_Neutral') as holiday_impact_tier,
        (ce.event_date is not null) as is_commercial_event,
        ce.commercial_event_name,
        coalesce(ce.commercial_event_tier, 'Tier_3_Neutral') as commercial_event_tier,
        (ds.full_date >= '2017-01-01' and ds.full_date < '2018-09-01') as is_clean_window_flag
    from date_spine ds
    left join holidays h
        on ds.full_date = h.holiday_date
    left join commercial_events ce
        on ds.full_date = ce.event_date
)

select
    (to_char(full_date, 'YYYYMMDD'))::int as date_key,
    *
from enriched
