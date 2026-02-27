WITH sellers AS (
  SELECT 
    seller_id,
    seller_zip_code_prefix,
    city,
    state
   FROM {{ ref('stg_sellers') }}
),
geolocation AS (
  SELECT 
    zip_code_prefix,
    AVG(latitude) as latitude,
    AVG(longitude) as longitude
  FROM {{ ref('stg_geolocation') }}
  GROUP BY zip_code_prefix
),
orders AS (
  SELECT 
    oi.seller_id,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_payment_value) as total_revenue
  FROM {{ ref('fct_orders') }} as o
  LEFT JOIN {{ ref('stg_order_items') }} as oi ON o.order_id = oi.order_id
  GROUP BY oi.seller_id
),
order_items AS (
  SELECT 
    seller_id,
    COUNT(product_id) as total_items_sold
  FROM {{ ref('stg_order_items') }}
  GROUP BY seller_id
),
reviews AS (
  SELECT 
    oi.seller_id,
    AVG(fo.review_score) as avg_review_score
  FROM {{ ref('stg_order_items') }} oi
  LEFT JOIN {{ ref('fct_orders') }} fo USING (order_id)
  GROUP BY oi.seller_id
),
final AS (
  SELECT
    sellers.seller_id,
    sellers.seller_zip_code_prefix,
    sellers.city,
    sellers.state,
    geolocation.latitude,
    geolocation.longitude,
    orders.total_orders,
    orders.total_revenue,
    order_items.total_items_sold, 
    reviews.avg_review_score
  FROM sellers
  LEFT JOIN geolocation ON sellers.seller_zip_code_prefix = geolocation.zip_code_prefix
  LEFT JOIN orders ON sellers.seller_id = orders.seller_id
  LEFT JOIN order_items ON sellers.seller_id = order_items.seller_id
  LEFT JOIN reviews ON sellers.seller_id = reviews.seller_id
)
SELECT * FROM final    