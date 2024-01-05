{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

WITH orders AS (
    SELECT *
    FROM {{ ref('base_orders') }}

), payments AS (
    SELECT *
    FROM {{ ref('base_payments') }}

), order_payments AS (
    SELECT 
        order_id
        {% for payment_method in payment_methods -%}
        , SUM(case when payment_method = '{{ payment_method }}' then amount else 0 end) AS {{ payment_method }}_amount
        {% endfor -%}
        , SUM(amount) AS total_amount
    FROM payments
    GROUP BY order_id

), result AS (
    SELECT orders.order_id
        , orders.customer_id
        , orders.order_date
        , orders.status

        {% for payment_method in payment_methods -%}
        , COALESCE(order_payments.{{ payment_method }}_amount, 0) AS {{ payment_method }}_amount
        {% endfor -%}

        , COALESCE(order_payments.total_amount, 0) AS amount
    FROM orders
    LEFT JOIN order_payments
        ON orders.order_id = order_payments.order_id

)
SELECT *
FROM result