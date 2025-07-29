with balance_manual as (

    select * from {{ ref('int_balance_manual_filled') }}

),

fx as (

    select * from {{ ref('int_fx_filled') }}

),

balance_manual_in_nok as (

    select
        b.date,
        b.asset_name,
        b.asset_id,
        {{ convert_to_nok('b.balance', 'b.currency', 'f') }} as balance,
        b.currency as original_currency

    from balance_manual as b
    left join fx as f on b.date = f.date

)

select * from balance_manual_in_nok
