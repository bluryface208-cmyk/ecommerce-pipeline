SELECT
    order_id,
    order_item_id::integer as order_item_id,
    product_id,
    seller_id,
    shipping_limit_date::timestamp_ntz as shipping_limit_date,
    price::numeric(10,2) as price,
    freight_value::numeric(10,2) as freight_value
FROM {{ source('raw', 'raw_order_items')}}