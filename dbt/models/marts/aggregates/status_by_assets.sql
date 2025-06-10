with invest_status as (

    select * from {{ ref('fct_status') }}

),

assets as (

    select * from {{ ref('stg_assets') }}

),

status_by_assets as (

    select
        asset_name,
        case
            when COUNTIF(purchase_amounts = 999999) > 0 then 999999
            else sum(purchase_amounts)
        end as purchase_amounts,
        SUM(purchase_sum) AS purchase_sum,
        case
            {# If purchase_amounts = 999999, all balance of the same assets in fct_status is identical.
                So avg(balance) here means any balance of an asset. 
                This assumption is true for the same asset, but not true for the same strategy.
            #}
            when countif(purchase_amounts = 999999) > 0 then avg(balance)
            when COUNTIF(balance = 999999) > 0 then 999999
            else SUM(balance)
        end as balance,
        CASE
            WHEN COUNTIF(purchase_amounts = 999999) > 0 THEN null
            ELSE SUM(purchase_sum) / NULLIF(SUM(purchase_amounts), 0)
        END as average_purchase_price
    from invest_status
    group by
        asset_name
),

status_by_assets_agg as (

    select
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
        average_purchase_price
    from status_by_assets

),

status_by_assets_agg_enriched as (

    select
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
        sba.average_purchase_price
    from status_by_assets_agg as sba
    left join assets as a on sba.asset_name = a.asset_name

)

select * from status_by_assets_agg_enriched
