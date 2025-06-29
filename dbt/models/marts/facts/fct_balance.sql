with purchase as (

    select * from {{ ref('fct_purchase') }}

),

asset_prices as (

    select * from {{ ref('int_asset_prices_merged_in_nok') }}

),

balance_manual as (

    select * from {{ ref('int_balance_manual_in_nok') }}

),

purchase_by_assets as (

    select
        date,
        asset_name,
        CASE
            WHEN COUNTIF(purchase_amounts = 999999) > 0 THEN 999999
            ELSE SUM(purchase_amounts)
        END AS purchase_amounts
    from purchase
    group by
        date,
        asset_name

),

asset_balance as (

    select
        a.date,
        a.asset_name,
        a.asset_id,
        a.price as asset_price,
        a.price * p.purchase_amounts as balance,
        a.original_currency
    from asset_prices as a
    left join purchase_by_assets as p
        on a.date = p.date
        and a.asset_name = p.asset_name
    where p.purchase_amounts != 999999

),

total_balance as (

    select
        ab.date,
        ab.asset_name,
        ab.asset_id,
        ab.asset_price,
        ab.balance,
        ab.original_currency
    from asset_balance as ab

    union all

    select
        bm.date,
        bm.asset_name,
        bm.asset_id,
        null as asset_price,
        bm.balance,
        bm.original_currency
    from balance_manual as bm

)

select * from total_balance
