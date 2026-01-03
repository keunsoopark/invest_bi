with status_assets as (
    select *
    from {{ ref('status_by_assets') }}
),

status_assets_with_month_end_date as (
    SELECT
        date,
        LAST_DAY(date, MONTH) AS month_end_date,
        asset_name,
        main_group,
        sub_group,
        region,
        purchase_sum,
        balance,
        profit,
        average_purchase_price
    from status_assets
),

status_assets_monthly_snapshot as (
    select
        FORMAT_DATE('%Y-%m', month_end_date) as month,
        month_end_date,
        asset_name,
        main_group,
        sub_group,
        region,
        purchase_sum,
        balance,
        profit as accumulated_profit,
        profit / NULLIF(purchase_sum, 0) * 100 as accumulated_profit_percentage,
        average_purchase_price
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY month_end_date, asset_name
                ORDER BY date DESC) AS rn
        FROM status_assets_with_month_end_date
    )
    WHERE rn = 1
        and month_end_date <= (SELECT MAX(date) FROM status_assets) -- exclude future months
),

status_assets_monthly_with_profit as (
    select
        *,
        accumulated_profit
          - lag(accumulated_profit) over (
                partition by asset_name
                order by month_end_date
            ) as monthly_profit
    from status_assets_monthly_snapshot
),

status_assets_monthly_with_profit_pct as (
    select
        *,
        safe_divide(
            monthly_profit,
            (balance + lag(balance) over (partition by asset_name order by month_end_date)) / 2
        ) * 100 as monthly_profit_percentage
    from status_assets_monthly_with_profit
)

select * from status_assets_monthly_with_profit_pct
