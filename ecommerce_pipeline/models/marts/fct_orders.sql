WITH orders AS (
  SELECT * FROM {{ ref('stg_orders') }}
),
payments AS (
  SELECT 
    order_id,
    SUM(payment_value) as total_payment_value
  FROM {{ ref('stg_payments') }}
  GROUP BY order_id
),
order_items AS (
  SELECT 
    order_id,
    COUNT(product_id) as total_items,
    SUM(price) as total_price
  FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),
reviews AS (
  SELECT 
    order_id,
    review_score  
  FROM {{ ref('stg_reviews') }}
  QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id 
        ORDER BY review_creation_date DESC
    ) = 1
),
final AS (
    SELECT
        orders.order_id,
        orders.customer_id,
        orders.order_status,
        orders.purchase_date,
        orders.delivered_customer_date,
        payments.total_payment_value,
        order_items.total_items,
        reviews.review_score,
        DATEDIFF('day', orders.purchase_date, orders.delivered_customer_date) as delivery_days,
        DATEDIFF('day', orders.purchase_date, orders.estimated_delivery_date) as estimated_delivery_days
    FROM orders
    LEFT JOIN payments USING (order_id)
    LEFT JOIN order_items USING (order_id)
    LEFT JOIN reviews USING (order_id)
)

SELECT * FROM final