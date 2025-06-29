with source as (

    select * from {{ source('src_external', 'fx') }}

),

renamed as (

    select
        date,
        usdnok,
        nokkrw

    from source

)

select * from renamed
