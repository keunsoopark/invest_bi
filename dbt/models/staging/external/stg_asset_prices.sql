with source as (

    select * from {{ source('src_external', 'asset_prices') }}

),

renamed as (

    select
        asset_name,
        asset_id,
        price,
        currency,
        date

    from source

)

select * from renamed
