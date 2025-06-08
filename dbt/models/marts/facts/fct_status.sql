with holdings as (

    select * from {{ ref('int_holdings') }}

),

assets as (

    select * from {{ ref('stg_assets') }}

),

holding_with_balance as (
    select
        h.asset_name,
        h.asset_id,
        h.strategy_name,
        h.strategy_details,
        h.holding_amounts,
        h.holding_sum,
        case
            when h.holding_amounts = 999999 then a.price
            else a.price * h.holding_amounts
        end as balance,
        h.average_holding_price,
    from holdings as h
    left join assets as a on h.asset_name = a.asset_name

),

invest_status as (
    select
        asset_name,
        asset_id,
        strategy_name,
        strategy_details,
        holding_amounts,
        holding_sum,
        balance
    from holding_with_balance
)

select * from invest_status
