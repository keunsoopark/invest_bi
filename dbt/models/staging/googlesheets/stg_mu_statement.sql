with company_statements as (
    select *
    from {{ source('src_googlesheets', 'company_statements') }}
),

mu_statement as (
    SELECT *
    FROM (
        SELECT
            month,
            item,
            value
        FROM company_statements
        WHERE company = 'MU'
    )
    PIVOT (
        SUM(value) FOR item IN (
            'operation balance' AS operation_balance,
            'reserve balance' AS reserve_balance,
            'folkeinvestering balance' AS folkeinvestering_balance,
            'klp balance' AS klp_balance,
            'nordnet balance' AS nordnet_balance,
            'debt from kp' AS debt_from_kp
        )
    )
),

mu_statement_enriched as (
    select
        month,
        DATE_SUB(DATE_ADD(PARSE_DATE('%Y-%m', month), INTERVAL 1 MONTH), INTERVAL 1 DAY) AS month_end_date,
        COALESCE(operation_balance, 0) as operation_balance,
        COALESCE(reserve_balance, 0) as reserve_balance,
        COALESCE(folkeinvestering_balance, 0) as folkeinvestering_balance,
        COALESCE(klp_balance, 0) as klp_balance,
        COALESCE(nordnet_balance, 0) as nordnet_balance,
        COALESCE(debt_from_kp, 0) as debt_from_kp
    from mu_statement
)

select * from mu_statement_enriched
