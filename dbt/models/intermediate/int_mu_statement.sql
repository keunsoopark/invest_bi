with mu_statement as (
    select *
    from {{ ref('stg_mu_statement') }}
),

mp_statement as (
    select *
    from {{ ref('stg_mp_statement') }}
),

mu_statement_enriched as (
    select mu.*,
        mp.debt_from_mu as debt_to_mp,
        100000 + mu.operation_balance + mp.debt_from_mu as business_operation_buffer 
    from mu_statement as mu
    left join mp_statement as mp
        on mu.month = mp.month
)

select * from mu_statement_enriched
