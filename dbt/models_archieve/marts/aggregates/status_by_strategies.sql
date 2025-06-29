with invest_status as (

    select * from {{ ref('fct_status') }}

),

status_by_strategies as (

    select
        strategy_name,
        case
            when COUNTIF(purchase_amounts = 999999) > 0 then 999999
            else sum(purchase_amounts)
        end as purchase_amounts,
        SUM(purchase_sum) AS purchase_sum,
        sum(balance) as balance,
        CASE
            WHEN COUNTIF(purchase_amounts = 999999) > 0 THEN null
            ELSE SUM(purchase_sum * purchase_amounts) / NULLIF(SUM(purchase_amounts), 0)
        END as average_purchase_price
    from invest_status
    group by
        strategy_name
),

status_by_strategies_agg as (

    select
        strategy_name,
        purchase_amounts,
        purchase_sum,
        balance,
        balance - purchase_sum as profit,
        case
            when purchase_amounts = 0 then null
            when ABS(balance) < 0.01 then null
            else (balance - purchase_sum) /purchase_sum * 100
        end as profit_percentage,
        average_purchase_price
    from status_by_strategies

)

select * from status_by_strategies_agg
