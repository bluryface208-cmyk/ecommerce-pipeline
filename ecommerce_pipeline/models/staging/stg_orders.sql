SELECT 
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp_ntz as purchase_date,
    order_approved_at::timestamp_ntz as approved_date,
    order_delivered_carrier_date::timestamp_ntz as delivered_carrier_date,
    order_delivered_customer_date::timestamp_ntz as delivered_customer_date,
    order_estimated_delivery_date::timestamp_ntz as estimated_delivery_date
FROM {{ source('raw', 'raw_orders')}}

