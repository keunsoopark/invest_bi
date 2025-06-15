with source as (

    select * from {{ source('src_googlesheets', 'assets') }}

),

renamed as (

    select
        asset_name,
        asset_id,
        main_group,
        sub_group,
        sector,
        region,
        sub_region,
        currency

    from source

)

select * from renamed
