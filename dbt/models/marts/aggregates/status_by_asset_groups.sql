with status_by_assets as (

    select * from {{ ref('status_by_assets') }}

),

status_by_asset_groups as (

    select
        main_group as asset_main_group,
        sum(purchase_sum) as purchase_sum,
        sum(balance) as balance,
        sum(profit) as profit
    from status_by_assets
    group by
        main_group

),

status_by_asset_groups_agg as(

    select 
        asset_main_group,
        purchase_sum,
        balance,
        balance / sum(balance) over () * 100 as balance_percentage,
        profit,
        case
            when purchase_sum = 0 then null
            when ABS(balance) < 0.01 then null
            else (balance - purchase_sum) / purchase_sum * 100
        end as profit_percentage
    from status_by_asset_groups

)

select * from status_by_asset_groups_agg
