SELECT 
    seller_id,
    seller_zip_code_prefix,
    seller_city as city,
    seller_state as state
FROM {{ source('raw', 'raw_sellers')}}