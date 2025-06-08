with invest_status as (

    select * from {{ ref('fct_status') }}

),

status_by_strategies as (

    select
        strategy_name,
        case
            when COUNTIF(holding_amounts = 999999) > 0 then 999999
            else sum(holding_amounts)
        end as holding_amounts,
        SUM(holding_sum) AS holding_sum,
        SUM(balance) AS balance,
        CASE
            WHEN COUNTIF(holding_amounts = 999999) > 0 THEN null
            ELSE SUM(holding_sum * holding_amounts) / NULLIF(SUM(holding_amounts), 0)
        END as average_holding_price
    from invest_status
    group by
        strategy_name
),

status_by_strategies_agg as (

    select
        strategy_name,
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
    from status_by_strategies

)

select * from status_by_strategies_agg
