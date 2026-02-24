WITH products AS (
  SELECT 
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
  FROM {{ ref('stg_products') }}
),
category_translation AS (
  SELECT 
    category_name_portuguese,
    category_name_english
  FROM {{ ref('stg_category_translation') }}
),
order_items AS (
  SELECT
    product_id,
    AVG(price) as avg_price,
    AVG(freight_value) as avg_freight_value,
    COUNT(order_id) as total_units_sold
  FROM {{ ref('stg_order_items') }}
  GROUP BY product_id
),
final AS (
  SELECT
    products.product_id,
    products.product_category_name,
    category_translation.category_name_english,
    products.product_name_length,
    products.product_description_length,
    products.product_photos_qty,
    products.product_weight_g,  
    products.product_length_cm,
    products.product_height_cm,
    products.product_width_cm,
    order_items.avg_price as price,
    order_items.avg_freight_value,
    order_items.total_units_sold
  FROM products
  LEFT JOIN category_translation ON products.product_category_name = category_translation.category_name_portuguese
  LEFT JOIN order_items ON products.product_id = order_items.product_id
)
SELECT * FROM final
ORDER BY product_id