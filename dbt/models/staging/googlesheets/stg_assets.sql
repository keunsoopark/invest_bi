with source as (

    select * from {{ source('src_googlesheets', 'assets') }}

),

renamed as (

    select
        TRIM(asset_name) as asset_name,
        TRIM(asset_id) as asset_id,
        main_group,
        sub_group,
        sector,
        region,
        sub_region,
        currency

    from source

)

select * from renamed
