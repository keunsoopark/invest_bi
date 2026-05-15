with source as (

    select * from {{ source('src_external', 'fx') }}

),

renamed as (

    select
        date,
        usdnok,
        eurnok,
        nokkrw

    from source

)

select * from renamed
