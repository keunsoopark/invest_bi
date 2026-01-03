with properties as (
    select *
    from {{ ref('fct_properties') }}
),

status_asset_groups as (
    select *
    from {{ ref('status_by_asset_groups') }}
),

fx as (
    select *
    from {{ ref('int_fx_filled') }}
),

status_asset_groups_with_month_end_date as (
    SELECT
        date,
        LAST_DAY(date, MONTH) AS month_end_date,
        -- CAST(DATE_SUB(DATE_TRUNC(date, MONTH) + INTERVAL 1 MONTH, INTERVAL 1 DAY) AS DATE) AS month_end_date,
        asset_main_group,
        purchase_sum,
        balance,
        profit
    from status_asset_groups
),

status_asset_groups_monthly_snapshot as (
    select
        date,
        month_end_date,
        asset_main_group,
        purchase_sum,
        balance,
        profit
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY month_end_date, asset_main_group 
                ORDER BY date DESC) AS rn
        FROM status_asset_groups_with_month_end_date
    )
    WHERE rn = 1
),

status_asset_groups_agg as (
    select
        month_end_date,
        sum(purchase_sum) as finance_purchase,
        sum(balance) as finance_balance,
        sum(profit) as finance_profit
    from status_asset_groups_monthly_snapshot
    group by month_end_date

),

status_asset_groups_agg2 as (
    select
        month_end_date,
        finance_purchase,
        finance_balance,
        finance_profit,
        finance_profit / finance_purchase as finance_profit_rate,
        finance_profit - LAG(finance_profit) OVER (ORDER BY month_end_date) AS finance_monthly_profit,
        finance_profit - LAG(finance_profit, 12) OVER (ORDER BY month_end_date) AS finance_yearly_profit
    from status_asset_groups_agg
),

status_properties as (
    select
        month,
        month_end_date,
        sum(property_value) as property_value,
        sum(debt) as property_debt,
        sum(property_equity) as property_equity,
        sum(monthly_capital_gain) as property_monthly_capital_gain,
        sum(yearly_capital_gain) as property_yearly_capital_gain,
        sum(total_capital_gain) as property_total_capital_gain,
        sum(total_acc_profit) as property_total_acc_profit
    from properties
    group by month, month_end_date
),

status_monthly as (
    select
        p.month,
        p.month_end_date,
        p.property_value,
        p.property_debt,
        p.property_equity,
        p.property_monthly_capital_gain,
        p.property_yearly_capital_gain,
        p.property_total_capital_gain,
        p.property_total_acc_profit,
        s.finance_purchase,
        s.finance_balance,
        s.finance_profit,
        s.finance_profit_rate,
        s.finance_monthly_profit,
        s.finance_yearly_profit,
        s.finance_balance - LAG(s.finance_balance) OVER (ORDER BY s.month_end_date) - s.finance_monthly_profit AS saving
    from status_properties p
    inner join status_asset_groups_agg2 s
        on p.month_end_date = s.month_end_date
),

status_monthly_agg as (
    select *,
        property_value + finance_balance as total_capital,
        property_equity + finance_balance as total_equity,
        property_debt / (property_value + finance_balance) as total_debt_ratio,
        finance_balance / (property_value + finance_balance) as finance_capital_ratio,
        property_value / (property_value + finance_balance) as property_capital_ratio,
        finance_balance / (property_equity + finance_balance) as finance_equity_ratio,
        property_equity / (property_equity + finance_balance) as property_equity_ratio,
        finance_monthly_profit / finance_balance as finance_monthly_profit_ratio,
        finance_yearly_profit / finance_balance as finance_yearly_profit_ratio,
        property_monthly_capital_gain / property_equity as property_monthly_profit_ratio,
        property_monthly_capital_gain / property_value as property_monthly_profit_ratio_capital,
        property_yearly_capital_gain / property_equity as property_yearly_profit_ratio,
        property_yearly_capital_gain / property_value as property_yearly_profit_ratio_capital,
        finance_monthly_profit + property_monthly_capital_gain as total_monthly_profit,
        (finance_monthly_profit + property_monthly_capital_gain) /
            (property_equity + finance_balance) as total_monthly_profit_ratio,
        finance_yearly_profit + property_yearly_capital_gain as total_yearly_profit,
        (finance_yearly_profit + property_yearly_capital_gain) /
            (property_equity + finance_balance) as total_yearly_profit_ratio,
        property_total_capital_gain + finance_profit as total_profit
    from status_monthly
),

status_monthly_agg_fx as (
    select s.*,
        s.total_equity / f.usdnok as total_equity_usd,
        s.total_equity * f.nokkrw as total_equity_krw
    from status_monthly_agg s
    left join fx f
        on s.month_end_date = f.date
)

select * from status_monthly_agg_fx
