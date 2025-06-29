with private_statement as (
    select *
    from {{ ref('stg_private_statement') }}
),

fx as (

    select * from {{ ref('int_fx_filled') }}

),

private_statement_in_nok as (

    select
        p.month,
        p.month_end_date,
        p.reserve_balance,
        p.public_investment_balance,
        p.korea_balance / f.nokkrw as korea_balance
    from private_statement as p
    left join fx as f on p.month_end_date = f.date

)

select * from private_statement_in_nok
