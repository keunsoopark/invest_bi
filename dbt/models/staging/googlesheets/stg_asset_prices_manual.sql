with source as (

    select * from {{ source('src_googlesheets', 'asset_prices_manual') }}

),

renamed as (

    select
        TRIM(asset_name) as asset_name,
        TRIM(asset_id) as asset_id,
        price,
        currency,
        updated_at as date

    from source

)

select * from renamed
