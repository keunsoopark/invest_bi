with private_statement as (
    select *
    from {{ source('src_googlesheets', 'private_statement') }}
),

private_statement_enriched as (
    select
        month,
        DATE_SUB(DATE_ADD(PARSE_DATE('%Y-%m', month), INTERVAL 1 MONTH), INTERVAL 1 DAY) AS month_end_date,
        COALESCE(reserve_balance, 0) as reserve_balance,
        COALESCE(public_investment_balance, 0) as public_investment_balance,
        COALESCE(korea_balance, 0) as korea_balance,
        no_debit_spend,
        no_credit_spend,
        kr_debit_spend
    from private_statement
)

select * from private_statement_enriched
