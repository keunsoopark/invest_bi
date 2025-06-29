with purchase as (

    select * from {{ ref('int_purchase') }}

),

assets as (

    select * from {{ ref('stg_assets') }}

),

purchase_with_balance as (
    select
        p.asset_name,
        p.asset_id,
        p.strategy_name,
        p.strategy_details,
        p.purchase_amounts,
        p.purchase_sum,
        case
            when p.purchase_amounts = 999999 then a.price
            else a.price * p.purchase_amounts
        end as balance,
        p.average_purchase_price,
    from purchase as p
    left join assets as a on p.asset_name = a.asset_name

),

invest_status as (
    select
        asset_name,
        asset_id,
        strategy_name,
        strategy_details,
        purchase_amounts,
        purchase_sum,
        balance
    from purchase_with_balance
)

select * from invest_status
