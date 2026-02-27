{% snapshot order_items_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key=['order_id','order_item_id'],
    strategy='check',
    check_cols=['price', 'freight_value']
) }}

SELECT * FROM {{ ref('stg_order_items') }}

{% endsnapshot %}