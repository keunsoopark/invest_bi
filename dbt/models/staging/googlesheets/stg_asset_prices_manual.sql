with source as (

    select * from {{ source('src_googlesheets', 'asset_prices_manual') }}

),

renamed as (

    select
        asset_name,
        asset_id,
        price,
        currency,
        updated_at as date

    from source

)

select * from renamed
