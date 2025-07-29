with invest_status as (

    select * from {{ ref('fct_status') }}

),

assets as (

    select * from {{ ref('stg_assets') }}

),

status_by_assets as (

    select
        date,
        asset_name,
        case
            when COUNTIF(purchase_amounts = 999999) > 0 then 999999
            else sum(purchase_amounts)
        end as purchase_amounts,
        SUM(purchase_sum) AS purchase_sum,
        case
            {# If purchase_amounts = 999999, the balance of an asset for a given date should be unique.
                So it does not matter either we use avg(balance) or max(balance) or whatever agg.
                And for these assets, there is a unique strategy_name and strategy_details as assumed in fct_status definition as well.
            #}
            when countif(purchase_amounts = 999999) > 0 then avg(balance)
            else SUM(balance)
        end as balance,
        -- Same as balance. There should be 1 asset price for an asset for a given date.
        avg(asset_price) as asset_price,
        CASE
            WHEN COUNTIF(purchase_amounts = 999999) > 0 THEN null
            ELSE SUM(purchase_sum) / NULLIF(SUM(purchase_amounts), 0)
        END as average_purchase_price
    from invest_status
    group by
        date,
        asset_name

),

status_by_assets_agg as (

    select
        date,
        asset_name,
        purchase_amounts,
        purchase_sum,
        balance,
        balance - purchase_sum as profit,
        case
            when purchase_amounts = 0 then null
            when ABS(balance) < 0.01 then null
            else (balance - purchase_sum) / purchase_sum * 100
        end as profit_percentage,
        asset_price - average_purchase_price as unrealized_profit_percentage,
        average_purchase_price
    from status_by_assets

),

status_by_assets_agg_enriched as (

    select
        sba.date,
        sba.asset_name,
        a.asset_id,
        a.main_group,
        a.sub_group,
        a.sector,
        a.region,
        a.sub_region,
        sba.purchase_amounts,
        sba.purchase_sum,
        sba.balance,
        sba.profit,
        sba.profit_percentage,
        sba.unrealized_profit_percentage,
        sba.average_purchase_price,
        a.currency
    from status_by_assets_agg as sba
    left join assets as a on sba.asset_name = a.asset_name

)

select * from status_by_assets_agg_enriched
