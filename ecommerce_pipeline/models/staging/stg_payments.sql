SELECT 
    order_id,
    payment_sequential::integer as payment_sequential,
    payment_type,
    payment_installments::integer as payment_installments,
    payment_value::numeric(10,2) as payment_value
FROM {{ source('raw', 'raw_payments')}}