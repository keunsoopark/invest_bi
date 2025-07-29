with asset_prices as (

    select * from {{ ref('int_asset_prices_filled') }}

),

asset_prices_manual as (

    select * from {{ ref('int_asset_prices_manual_filled') }}

),

fx as (

    select * from {{ ref('int_fx_filled') }}

),

asset_prices_merged as (
    
    select
        ap.date,
        ap.asset_name,
        ap.asset_id,
        ap.price,
        ap.currency
    from asset_prices as ap

    union all

    select
        apm.date,
        apm.asset_name,
        apm.asset_id,
        apm.price,
        apm.currency
    from asset_prices_manual as apm
),

asset_prices_merged_in_nok as (

    select
        a.date,
        a.asset_name,
        a.asset_id,
        {{ convert_to_nok('a.price', 'a.currency', 'f') }} as price,
        a.currency as original_currency

    from asset_prices_merged as a
    left join fx as f on a.date = f.date

)

select * from asset_prices_merged_in_nok
