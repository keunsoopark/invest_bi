with transactions as (

    select * from {{ ref('stg_transactions') }}

),

assets as (

    select * from {{ ref('stg_assets') }}

),

holdings as (
    select
        asset_name,
        strategy_name,
        strategy_details,
        case
            when COUNTIF(amounts = 999999) > 0 then 999999
            else sum(amounts)
        end as holding_amounts,
        SUM(
            CASE
                WHEN amounts = 999999 THEN price
                ELSE price * amounts
            END
        ) AS holding_sum,
        CASE
            WHEN COUNTIF(amounts = 999999) > 0 THEN null
            ELSE SUM(price * amounts) / NULLIF(SUM(amounts), 0)
        END as average_holding_price
    from transactions
    group by
        asset_name,
        strategy_name,
        strategy_details
),

holdings_enriched as (
    select
        hba.asset_name,
        a.asset_id,
        hba.strategy_name,
        hba.strategy_details,
        hba.holding_amounts,
        hba.holding_sum,
        hba.average_holding_price,
    from holdings as hba
    left join assets as a on hba.asset_name = a.asset_name
)

select * from holdings_enriched
