with private_statement as (
    select *
    from {{ ref('int_private_statement_in_nok') }}
),

mu_statement as (
    select *
    from {{ ref('int_mu_statement') }}
),

fct_balance as (
    select *
    from {{ ref('fct_balance') }}
),

fct_balance_selected AS (
    SELECT
        date,
        SUM(CASE WHEN asset_name IN ('USDT', 'SGOV') THEN balance ELSE 0 END) AS usd_cash,
        SUM(CASE WHEN asset_name = 'crypto' THEN balance ELSE 0 END) AS crypto,
        SUM(CASE WHEN asset_name = '김프 짤짤이' THEN balance ELSE 0 END) AS no_invest_ready_balance,
        SUM(CASE WHEN asset_name = 'KRW현금' THEN balance ELSE 0 END) AS krw_cash
    FROM fct_balance
    GROUP BY date
),

private_statement_enriched as (
    select
        ps.month,
        ps.month_end_date,
        ps.reserve_balance,
        ps.public_investment_balance,
        f.usd_cash,
        f.crypto,
        f.no_invest_ready_balance,
        ps.korea_balance,
        f.krw_cash,
        mu.debt_from_kp as loan_to_mu,
        mu.operation_balance + mu.reserve_balance + mu.folkeinvestering_balance + mu.klp_balance - mu.debt_from_kp + mu.debt_to_mp as total_mu_balance,
        mu.business_operation_buffer
    from private_statement ps
    left join mu_statement mu on ps.month = mu.month
    left join fct_balance_selected f on ps.month_end_date = f.date
),

private_statement_agg as (
    select *,
        reserve_balance + public_investment_balance + usd_cash + crypto + no_invest_ready_balance + korea_balance + krw_cash + loan_to_mu as private_balance,
        (reserve_balance + public_investment_balance + usd_cash + crypto + no_invest_ready_balance + korea_balance + krw_cash + loan_to_mu) + total_mu_balance - business_operation_buffer as financial_invest_balance
    from private_statement_enriched
)

select * from private_statement_agg
