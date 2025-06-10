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
        a.main_group,
        a.sub_group,
        a.sector,
        a.region,
        a.sub_region,
        case
            when a.currency = 'NOK' then a.price
            when a.currency = 'USD' then a.price * fx_latest.usdnok
            when a.currency = 'KRW' then a.price / fx_latest.nokkrw
        end as price,
        a.currency as original_currency,
        a.updated_at

    from assets as a
    LEFT JOIN (
        SELECT *
        FROM fx
        WHERE date = (SELECT MAX(date) FROM fx)
    ) AS fx_latest
    ON TRUE     -- This assumes that the latest FX rates are applied to all assets - i.e., single row cross join

)

select * from assets_in_nok
