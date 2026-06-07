with invest_status as (

    select * from {{ ref('fct_status') }}

),

status_by_strategy_details as (

    select
        date,
        strategy_details,
        SUM(purchase_sum) AS purchase_sum,
        sum(balance) as balance
    from invest_status
    group by
        date,
        strategy_details
),

status_by_strategy_details_agg as (

    select
        date,
        strategy_details,
        purchase_sum,
        balance,
        balance - purchase_sum as profit,
        case
            when purchase_sum = 0 then null
            when ABS(balance) < 0.01 then null
            else (balance - purchase_sum) / purchase_sum * 100
        end as profit_percentage
    from status_by_strategy_details

)

select * from status_by_strategy_details_agg
