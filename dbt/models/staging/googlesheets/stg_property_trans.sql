with source as (

    select * from {{ source('src_googlesheets', 'property_trans') }}

),

renamed as (

    select
        date,
        TRIM(name) as property_name,
        TRIM(item) as item,
        price

    from source

)

select * from renamed
