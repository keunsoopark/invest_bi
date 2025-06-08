with assets as (

    select * from {{ ref('base_assets') }}

),

fx as (

    select * from {{ ref('stg_fx') }}

),

assets_in_nok as (

    select
        a.asset_name,
        a.asset_id,
        a.main_class,
        a.sub_class,
        a.sector,
        a.region,
        a.sub_region,
        case
            when a.currency = 'NOK' then a.price
            when a.currency = 'USD' then a.price * f.usdnok
            when a.currency = 'KRW' then a.price / f.nokkrw
        end as price,
        a.currency as original_currency,
        a.updated_at

    from assets as a
    left join fx as f on a.updated_at = f.date

)

select * from assets_in_nok
