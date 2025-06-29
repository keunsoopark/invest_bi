with source as (

    select * from {{ source('src_googlesheets', 'property_trans') }}

),

renamed as (

    select
        date,
        name as property_name,
        item,
        price

    from source

)

select * from renamed
