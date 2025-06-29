with transactions as (

    select * from {{ ref('int_transactions_in_nok') }}

),

assets as (

    select * from {{ ref('stg_assets') }}

),

transactions_enriched AS (

    SELECT
        t.date,
        t.asset_name,
        t.asset_id,
        t.price,
        t.original_currency,
        t.amounts,
        t.strategy_name,
        t.strategy_details,
        a.main_group AS asset_main_group,
        a.sub_group AS asset_sub_group,
        a.sector AS sector,
        a.region AS region,
        a.sub_region AS sub_region,
        CASE 
            WHEN t.amounts = 999999 THEN t.price
            ELSE t.price * t.amounts
        END AS purchase
    FROM transactions AS t
    LEFT JOIN assets AS a
        ON t.asset_name = a.asset_name

)

select * from transactions_enriched
