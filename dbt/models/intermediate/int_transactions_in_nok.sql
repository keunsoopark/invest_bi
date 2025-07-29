with transactions as (

    select * from {{ ref('stg_transactions') }}

),

fx as (

    select * from {{ ref('int_fx_filled') }}

),

transactions_in_nok as (

    select
        t.date,
        t.asset_name,
        t.asset_id,
        {{ convert_to_nok('t.price', 't.currency', 'f') }} as price,
        t.currency as original_currency,
        t.amounts,
        t.strategy_name,
        t.strategy_details,

    from transactions as t
    left join fx as f on t.date = f.date

)

select * from transactions_in_nok
