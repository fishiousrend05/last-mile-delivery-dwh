{% snapshot snap_driver %}

{{
    config(
      target_schema='snapshots',
      unique_key='driver_id',
      strategy='check',
      check_cols=['driver_status', 'vehicle_type', 'home_zone_id'],
    )
}}

select * from {{ ref('stg_synthetic__drivers') }}

{% endsnapshot %}