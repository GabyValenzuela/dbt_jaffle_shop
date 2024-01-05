WITH source AS (
    SELECT *
    FROM {{ ref('raw_orders') }}

), transformed AS (
    SELECT 
        CAST(order_id AS {{ type_string() }}) AS order_id
        , CAST(customer_id AS {{ type_string() }}) AS customer_id
        , CAST(order_date AS DATE) AS order_date
        , LOWER(status) AS status
    FROM source
    
)
SELECT *
FROM transformed