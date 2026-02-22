SELECT 
    product_category_name as category_name_portuguese,
    product_category_name_english as category_name_english
FROM {{ source('raw', 'raw_category_translation')}}