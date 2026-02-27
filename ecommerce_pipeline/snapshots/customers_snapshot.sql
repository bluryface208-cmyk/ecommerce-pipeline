{% snapshot customers_snapshot%}

{{
    config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['customer_unique_id', 'customer_zip_code_prefix', 'city', 'state']
    )
}}

SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}