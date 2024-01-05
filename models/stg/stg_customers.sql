WITH source AS (
    SELECT *
    FROM {{ ref('raw_customers') }}

), transformed AS (
    SELECT 
        CAST(customer_id AS {{ type_string() }}) AS customer_id
        , LOWER(first_name) AS first_name
        , LOWER(last_name) AS last_name
    FROM source
    
)
SELECT *
FROM transformed