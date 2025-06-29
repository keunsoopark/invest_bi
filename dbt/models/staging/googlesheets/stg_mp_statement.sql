with company_statements as (
    select *
    from {{ source('src_googlesheets', 'company_statements') }}
),

mp_statement as (
    SELECT *
    FROM (
        SELECT
            month,
            item,
            value
        FROM company_statements
        WHERE company = 'MP'
    )
    PIVOT (
        SUM(value) FOR item IN (
            'operation balance' AS operation_balance,
            'debt from mu' AS debt_from_mu,
            'car debt' AS car_debt
        )
    )
),

mp_statement_enriched as (
    select
        month,
        DATE_SUB(DATE_ADD(PARSE_DATE('%Y-%m', month), INTERVAL 1 MONTH), INTERVAL 1 DAY) AS month_end_date,
        COALESCE(operation_balance, 0) as operation_balance,
        COALESCE(debt_from_mu, 0) as debt_from_mu,
        COALESCE(car_debt, 0) as car_debt
    from mp_statement
)

select * from mp_statement_enriched
