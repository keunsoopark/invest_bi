with invest_status as (

    select * from {{ ref('fct_status') }}

),

status_by_strategies as (

    select
        date,
        strategy_name,
        SUM(purchase_sum) AS purchase_sum,
        sum(balance) as balance
    from invest_status
    group by
        date,
        strategy_name
),

status_by_strategies_agg as (

    select
        date,
        strategy_name,
        purchase_sum,
        balance,
        balance - purchase_sum as profit,
        case
            when ABS(balance) < 0.01 then null
            else (balance - purchase_sum) / purchase_sum * 100
        end as profit_percentage
    from status_by_strategies

)

select * from status_by_strategies_agg
