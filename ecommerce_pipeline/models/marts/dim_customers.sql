WITH customers AS (
  SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    city,
    state
   FROM {{ ref('stg_customers') }}
),
orders AS (
  SELECT 
    customer_id,
    MIN(purchase_date) as first_purchase_date,
    MAX(purchase_date) as last_purchase_date,
    COUNT(order_id) as total_orders,
    SUM(total_payment_value) as lifetime_value,
    AVG(review_score) as avg_review_score
  FROM {{ ref('fct_orders') }}
  GROUP BY customer_id
),
final AS (
  SELECT
    customers.customer_id,
    customers.customer_unique_id,
    customers.customer_zip_code_prefix,
    customers.city,
    customers.state,
    orders.first_purchase_date,
    orders.last_purchase_date,
    orders.total_orders,
    orders.lifetime_value,
    orders.avg_review_score,    
    DATEDIFF('day', orders.first_purchase_date, orders.last_purchase_date) as customer_lifespan_days,
    CASE 
      WHEN orders.total_orders = 0 THEN 0
      ELSE orders.lifetime_value / orders.total_orders 
    END as avg_order_value 
  FROM customers    
  LEFT JOIN orders ON customers.customer_id = orders.customer_id
)
SELECT * FROM final