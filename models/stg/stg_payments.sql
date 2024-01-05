WITH source AS (
    SELECT *
    FROM {{ ref('raw_payments') }}

), transformed AS (
    SELECT CAST(payment_id AS {{ type_string() }}) AS payment_id
        , CAST(order_id AS {{ type_string() }}) AS order_id
        , LOWER(payment_method) AS payment_method
        , CAST(amount AS {{ type_float() }}) AS amount
    FROM source
    
)
SELECT *
FROM transformed