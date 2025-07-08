with purchase as (
    select *
    from {{ ref('fct_purchase') }} p
),

balance as (
    select *
    from {{ ref('fct_balance') }} b
),

assets as (
    select *
    from {{ ref('stg_assets') }}
),

invest_status as (
    select
        p.date,
        p.asset_name,
        b.asset_id,
        p.strategy_name,
        p.strategy_details,
        p.purchase_amounts,
        p.purchase_sum,
        case
            when p.purchase_amounts = 999999 then b.balance
            else b.asset_price * p.purchase_amounts
        end as balance,
        p.average_purchase_price
    from purchase as p
    left join balance as b 
        on p.date = b.date
        and p.asset_name = b.asset_name
),

invest_status_enriched as (
    select i.*,
        a.main_group,
        a.sub_group,
        a.sector,
        a.region,
        a.sub_region,
        a.currency
    from invest_status i
    left join assets a
    on i.asset_name = a.asset_name
)

select * from invest_status_enriched
