with purchase as (

    select * from {{ ref('fct_purchase') }}

),

asset_prices as (

    select * from {{ ref('int_asset_prices_merged_in_nok') }}

),

balance_manual as (

    select * from {{ ref('int_balance_manual_in_nok') }}

),

assets as (
    select *
    from {{ ref('stg_assets') }}
),

countable_assets_balance as (

    select
        p.date,
        p.asset_name,
        p.asset_id,
        p.strategy_name,
        p.strategy_details,
        p.purchase_amounts,
        p.purchase_sum,
        a.price as asset_price,
        a.price * p.purchase_amounts as balance,
        a.original_currency
        {# p.average_purchase_price might not be needed here, cuz it should be recalculated for other group by.
            For example, this is recalculated in status_by_assets cuz they need per asset view.
            They need purchase_sum and purchase_amounts for each asset, so avg_purchase_price here is useless in that step.
        #}
    from purchase as p
    left join asset_prices as a
        on p.date = a.date
        and p.asset_name = a.asset_name
    where p.purchase_amounts != 999999

),

balance_manual_enriched as (

    select
        bm.date,
        bm.asset_name,
        bm.asset_id,
        p.strategy_name,
        p.strategy_details,
        p.purchase_amounts,
        p.purchase_sum,
        null as asset_price,
        bm.balance,
        bm.original_currency
    from balance_manual as bm
    left join purchase as p
        on bm.date = p.date
        and bm.asset_name = p.asset_name
    where p.purchase_amounts = 999999

),

total_status as (

    select
        ab.date,
        ab.asset_name,
        ab.asset_id,
        ab.strategy_name,
        ab.strategy_details,
        ab.purchase_amounts,
        ab.purchase_sum,
        ab.asset_price,
        ab.balance,
        ab.original_currency
    from countable_assets_balance as ab

    union all

    select
        bm.date,
        bm.asset_name,
        bm.asset_id,
        bm.strategy_name,
        bm.strategy_details,
        bm.purchase_amounts,
        bm.purchase_sum,
        bm.asset_price,
        bm.balance,
        bm.original_currency
    from balance_manual_enriched as bm

),

total_status_enriched as (
    select ts.*,
        a.main_group,
        a.sub_group,
        a.sector,
        a.region,
        a.sub_region,
        a.currency
    from total_status ts
    left join assets a
        on ts.asset_name = a.asset_name
)

select * from total_status_enriched
