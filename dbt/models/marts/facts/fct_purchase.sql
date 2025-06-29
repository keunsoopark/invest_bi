WITH transactions AS (
    SELECT 
        date,
        asset_name,
        strategy_name,
        strategy_details,
        amounts,
        price
    FROM {{ ref('int_transactions_in_nok') }}
),

assets AS (
    SELECT * 
    FROM {{ ref('stg_assets') }}
),

first_txn_date AS (
    SELECT
        asset_name,
        strategy_name,
        strategy_details,
        MIN(date) AS min_date
    FROM transactions
    GROUP BY asset_name, strategy_name, strategy_details
),

date_range AS (
    SELECT
        a.asset_name,
        a.strategy_name,
        a.strategy_details,
        d AS date
    FROM first_txn_date a
    CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(
        a.min_date,
        CURRENT_DATE() - 1
    )) AS d
),

daily_transactions AS (
    SELECT
        date,
        asset_name,
        strategy_name,
        strategy_details,
        case
            WHEN COUNTIF(amounts = 999999) > 0 THEN 999999
            else sum(amounts)
        end as amounts,
        case
            WHEN COUNTIF(amounts = 999999) > 0 THEN sum(price)
            ELSE SUM(price * amounts)
        END AS purchase_sum
    FROM transactions
    GROUP BY date, asset_name, strategy_name, strategy_details
),

transaction_daily_filled AS (
    SELECT
        dr.date,
        dr.asset_name,
        dr.strategy_name,
        dr.strategy_details,
        txn.amounts,
        txn.purchase_sum
    FROM date_range dr
    LEFT JOIN daily_transactions txn
        ON dr.date = txn.date
        AND dr.asset_name = txn.asset_name
        AND dr.strategy_name = txn.strategy_name
        AND COALESCE(dr.strategy_details, '') = COALESCE(txn.strategy_details, '')
),

-- Track whether 999999 occurred, and compute running sums
purchase_daily_sum_temp AS (
    SELECT
        date,
        asset_name,
        strategy_name,
        strategy_details,

        MAX(CASE WHEN amounts = 999999 THEN 1 ELSE 0 END) OVER (
            PARTITION BY asset_name, strategy_name, strategy_details
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS has_infinite_flag,

        SUM(
            CASE WHEN amounts IS NULL OR amounts = 999999 THEN 0 ELSE amounts END
        ) OVER (
            PARTITION BY asset_name, strategy_name, strategy_details
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS finite_amounts_sum,

        SUM(purchase_sum) OVER (
            PARTITION BY asset_name, strategy_name, strategy_details
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS purchase_sum
    FROM transaction_daily_filled
),

-- Final values with correct 999999 propagation
purchase_daily_sum AS (
    SELECT
        t.date,
        t.asset_name,
        sa.asset_id,
        t.strategy_name,
        t.strategy_details,

        CASE
            WHEN t.has_infinite_flag = 1 THEN 999999
            ELSE t.finite_amounts_sum
        END AS purchase_amounts,

        t.purchase_sum,

        CASE
            WHEN t.has_infinite_flag = 1 THEN NULL
            WHEN t.finite_amounts_sum > 0 THEN ROUND(t.purchase_sum / t.finite_amounts_sum, 2)
            ELSE NULL
        END AS average_purchase_price

    FROM purchase_daily_sum_temp t
    LEFT JOIN assets sa ON t.asset_name = sa.asset_name
)

SELECT *
FROM purchase_daily_sum
