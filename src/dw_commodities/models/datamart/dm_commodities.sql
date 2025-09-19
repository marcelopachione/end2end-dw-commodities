-- Table --> dm_commodities

WITH commodities AS (
    SELECT
        date,
        ticker,
        closing_value
    FROM
        {{ ref("stg_commodities") }}
), moviment AS (
    SELECT
        date,
        ticker,
        action,
        quantity
    FROM
        {{ ref('stg_moviment_commodities') }}
), joined AS (
    SELECT 
        t1.date AS date,
        t1.ticker,
        t1.closing_value,
        t2.action,
        t2.quantity,
        ROUND((t2.quantity * t1.closing_value)::numeric, 2) AS "value",
        CASE
            WHEN t2.action = 'sell' 
                THEN  ROUND((t2.quantity * t1.closing_value)::numeric, 2)
            ELSE - ROUND((t2.quantity * t1.closing_value)::numeric, 2)
        END AS profit
    FROM
        commodities t1
    INNER JOIN
        moviment t2
            ON t1.date = t2.date AND t1.ticker = t2.ticker
), last_day AS (
    SELECT
        MAX(j.date) AS max_date
    FROM
        joined j
), filtered AS (
    SELECT
        *
    FROM
        joined
    WHERE
        date = (SELECT max_date FROM last_day)
)
SELECT
    date,
    ticker, 
    closing_value,
    action,
    quantity,
    value,
    profit
FROM
    filtered