with transactions as (

    select * from {{ ref('stg_transactions') }}

),

assets as (

    select * from {{ ref('stg_assets') }}

),

purchase as (
    select
        asset_name,
        strategy_name,
        strategy_details,
        case
            when COUNTIF(amounts = 999999) > 0 then 999999
            else sum(amounts)
        end as purchase_amounts,
        SUM(
            CASE
                WHEN amounts = 999999 THEN price
                ELSE price * amounts
            END
        ) AS purchase_sum,
        CASE
            WHEN COUNTIF(amounts = 999999) > 0 THEN null
            ELSE SUM(price * amounts) / NULLIF(SUM(amounts), 0)
        END as average_purchase_price
    from transactions
    group by
        asset_name,
        strategy_name,
        strategy_details
),

purchase_enriched as (
    select
        p.asset_name,
        a.asset_id,
        p.strategy_name,
        p.strategy_details,
        p.purchase_amounts,
        p.purchase_sum,
        p.average_purchase_price,
    from purchase as p
    left join assets as a on p.asset_name = a.asset_name
)

select * from purchase_enriched
