with source as (

    select * from {{ source('src_googlesheets', 'properties') }}

),

renamed as (

    select
        TRIM(name) as property_name,
        TRIM(location) as property_location,
        p_rom,
        construction_year,
        soverom,
        etasje,
        purchase_price,
        initial_debt,
        purchase_year

    from source

)

select * from renamed
