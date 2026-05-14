{% set megler_cost = 200000 %}

with property_hist as (
    select *
    from {{ ref('int_property_hist') }}
),

property_trans as (
    select *
    from {{ ref('stg_property_trans') }}
),

properties as (
    select *
    from {{ ref('stg_properties') }}
),

property_hist_w_lags AS (
    SELECT
        *,
        LAG(property_value) OVER (PARTITION BY property_name ORDER BY date) AS prev_month_value,
        LAG(property_value, 12) OVER (PARTITION BY property_name ORDER BY date) AS prev_year_value
    FROM property_hist
),

property_hist_agg as (
    select
        FORMAT_DATE('%Y-%m', date) AS month,
        CAST(DATE_SUB(DATE_TRUNC(date, MONTH) + INTERVAL 1 MONTH, INTERVAL 1 DAY) AS DATE) AS month_end_date,
        property_name,
        property_value,
        debt,
        property_value - debt - {{ megler_cost }} as property_equity,
        debt / property_value * 100 as debt_ratio,

        property_value - prev_month_value AS monthly_capital_gain,
        SAFE_DIVIDE(property_value - prev_month_value, prev_month_value) * 100 AS monthly_capital_gain_ratio,
        property_value - prev_year_value AS yearly_capital_gain,
        SAFE_DIVIDE(property_value - prev_year_value, prev_year_value) * 100 AS yearly_capital_gain_ratio,

        tenant,
        monthly_rent,
        loan_payback,
        loan_cost,
        loan_payback + loan_cost as loan_cashflow,
        interest_rate,
        felleskostnad,
        garbage_cost,
        forsikring_cost,
        water_cost,
        electricity_cost,
        internet_cost,
        rent_mgmt_cost,
        depreciation_cost,
        eiendomsskatt_bolig,
        -- water cost is not considered as my cost, cuz it is paid by the tenant
        loan_cost + felleskostnad + garbage_cost + forsikring_cost + electricity_cost + internet_cost + rent_mgmt_cost + depreciation_cost + eiendomsskatt_bolig as total_cost,
        loan_payback + loan_cost + felleskostnad + garbage_cost + forsikring_cost + electricity_cost + internet_cost + rent_mgmt_cost + depreciation_cost + eiendomsskatt_bolig as total_cash_outflow,
        monthly_rent - (loan_cost + felleskostnad + garbage_cost + forsikring_cost + electricity_cost + internet_cost + rent_mgmt_cost + depreciation_cost + eiendomsskatt_bolig) as rent_net_income,
        monthly_rent - (loan_payback + loan_cost + felleskostnad + garbage_cost + forsikring_cost + electricity_cost + internet_cost + rent_mgmt_cost + depreciation_cost + eiendomsskatt_bolig) as total_cashflow,
        monthly_rent * 12 / property_value * 100 as rent_nominal_yield
    from property_hist_w_lags
),

property_hist_agg2 as (
    select *,
        rent_net_income * 12 / property_value * 100 as rent_net_yield,
        monthly_rent * 12 / property_equity * 100 as rent_nominal_equity_yield,
        rent_net_income * 12 / property_equity * 100 as rent_net_equity_yield
    from property_hist_agg
),

property_monthly_trans as (
    select
        FORMAT_DATE('%Y-%m', date) AS month,
        property_name,
        SUM(price) as transactions
    from property_trans
    group by month, property_name
),

property_monthly_joined as (
    select
        h.*,
        COALESCE(t.transactions, 0) as transactions
    from property_hist_agg2 as h
    left join property_monthly_trans as t
        on h.month = t.month
        and h.property_name = t.property_name
),

property_monthly_status as (
    select
        month,
        month_end_date,
        property_name,
        property_value,
        debt,
        property_equity,
        debt_ratio,
        COALESCE(monthly_capital_gain, 0) as monthly_capital_gain,
        monthly_capital_gain_ratio,
        yearly_capital_gain,
        yearly_capital_gain_ratio,
        tenant,
        monthly_rent,
        loan_payback,
        loan_cost,
        loan_cashflow,
        interest_rate,
        felleskostnad,
        garbage_cost,
        forsikring_cost,
        water_cost,
        electricity_cost,
        internet_cost,
        rent_mgmt_cost,
        depreciation_cost,
        eiendomsskatt_bolig,
        -- minus in transactions are plus in cost
        total_cost - transactions as total_cost,
        total_cash_outflow - transactions as total_cash_outflow,
        rent_net_income + transactions as rent_net_income,
        total_cashflow + transactions as total_cashflow,
        rent_nominal_yield,
        rent_net_yield,
        rent_nominal_equity_yield,
        rent_net_equity_yield,
        transactions
    from property_monthly_joined

),

property_monthly_status_total_amounts as (
    select
        pms.*,
        pms.property_value - pr.purchase_price as total_capital_gain,
        pr.initial_debt - pms.debt as total_debt_repayment,
        (pr.initial_debt - pms.debt) / pr.initial_debt * 100 as total_debt_repayment_ratio
    from property_monthly_status as pms
    left join properties as pr
        on pms.property_name = pr.property_name
),

property_monthly_status_accumulated as (
    SELECT *,
        SUM(rent_net_income) OVER (
            PARTITION BY property_name
            ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) + total_capital_gain AS total_acc_profit
    FROM property_monthly_status_total_amounts
)

select * from property_monthly_status_accumulated
