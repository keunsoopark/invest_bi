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
            when COUNTIF(holding_amounts = 999999) > 0 then 999999
            else sum(holding_amounts)
        end as holding_amounts,
        SUM(holding_sum) AS holding_sum,
        SUM(balance) AS balance,
        CASE
            WHEN COUNTIF(holding_amounts = 999999) > 0 THEN null
            ELSE SUM(holding_sum) / NULLIF(SUM(holding_amounts), 0)
        END as average_holding_price
    from invest_status
    group by
        asset_name
),

status_by_assets_agg as (

    select
        asset_name,
        holding_amounts,
        holding_sum,
        balance,
        balance - holding_sum as profit,
        case
            when holding_amounts = 0 then null
            when ABS(balance) < 0.01 then null
            else (balance - holding_sum) /holding_sum * 100
        end as profit_percentage,
        average_holding_price
    from status_by_assets

),

status_by_assets_agg_enriched as (

    select
        sba.asset_name,
        a.asset_id,
        a.main_class,
        a.sub_class,
        a.sector,
        a.region,
        a.sub_region,
        sba.holding_amounts,
        sba.holding_sum,
        sba.balance,
        sba.profit,
        sba.profit_percentage,
        sba.average_holding_price
    from status_by_assets_agg as sba
    left join assets as a on sba.asset_name = a.asset_name

)

select * from status_by_assets_agg_enriched
