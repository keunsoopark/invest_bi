with source as (

    select * from {{ source('src_external', 'asset_prices') }}

),

renamed as (

    select
        date,
        asset_name,
        asset_id,
        price,
        currency

    from source

)

select * from renamed
