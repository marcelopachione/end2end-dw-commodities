-- Table --> commodities

WITH source AS (
    SELECT
        "date",
        "close",
        "ticker"
    FROM {{ source('dw_commodities', 'commodities') }}
), renamed AS (
    SELECT
        CAST("date" AS date),
        "close" AS closing_value,
        "ticker"
    FROM source
)
SELECT * FROM renamed

