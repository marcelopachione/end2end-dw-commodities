-- Table --> moviment_commodities

WITH source AS (
    SELECT * FROM {{ source('dw_commodities', 'moviment_commodities') }}
), renamed AS (
    SELECT
        CAST("date" AS date),
        "symbol" AS ticker,
        action,
        quantity
    FROM source
)
SELECT * FROM renamed