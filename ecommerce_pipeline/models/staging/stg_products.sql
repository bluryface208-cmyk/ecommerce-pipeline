SELECT 
    product_id,
    product_category_name,
    PRODUCT_NAME_LENGHT::integer as product_name_length,
    PRODUCT_DESCRIPTION_LENGHT::integer as product_description_length,
    product_photos_qty::integer as product_photos_qty,
    product_weight_g::numeric(10,2) as product_weight_g,
    product_length_cm::numeric(10,2) as product_length_cm,
    product_height_cm::numeric(10,2) as product_height_cm,
    product_width_cm::numeric(10,2) as product_width_cm
FROM {{ source('raw', 'raw_products')}}