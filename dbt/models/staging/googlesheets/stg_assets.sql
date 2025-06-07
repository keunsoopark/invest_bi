with source as (

    select * from {{ source('src_googlesheets', 'assets') }}

),

renamed as (

    select
        asset_name,
        asset_id,
        main_class,
        sub_class,
        sector,
        region,
        sub_region

    from source

)

select * from renamed
