WITH customers AS (
    SELECT *
    FROM {{ ref('stg_customers') }}

), orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}

), payments AS (
    SELECT *
    FROM {{ ref('stg_payments') }}

), customers_orders AS (
    SELECT 
        customer_id
        , MIN(order_date) AS first_order
        , MAX(order_date) AS most_recent_order
        , COUNT(order_id) AS number_of_orders
    FROM orders
    GROUP BY customer_id

), customer_payments AS (
    SELECT orders.customer_id
        , SUM(payments.amount) AS total_amount
    FROM payments
    LEFT JOIN orders
        ON payments.order_id = orders.order_id

    GROUP BY orders.customer_id

), result AS (
    SELECT customers.customer_id
        , customers.first_name
        , customers.last_name
        , customers_orders.first_order
        , customers_orders.most_recent_order
        , customers_orders.number_of_orders
        , customer_payments.total_amount AS customer_lifetime_value
    FROM customers
    LEFT JOIN customers_orders
        ON customers.customer_id = customers_orders.customer_id
    LEFT JOIN customer_payments
        ON  customers.customer_id = customer_payments.customer_id

)
SELECT *
FROM result