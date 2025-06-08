with transactions as (

    select * from {{ ref('base_transactions') }}

),

fx as (

    select * from {{ ref('stg_fx') }}

),

transactions_in_nok as (

    select
        t.date,
        t.asset_name,
        t.asset_id,
        case
            when t.currency = 'NOK' then t.price
            when t.currency = 'USD' then t.price * f.usdnok
            when t.currency = 'KRW' then t.price / f.nokkrw
        end as price,
        t.currency as original_currency,
        t.amounts,
        t.strategy_name,
        t.strategy_details,
        t.transaction_version

    from transactions as t
    left join fx as f on t.date = f.date

)

select * from transactions_in_nok
