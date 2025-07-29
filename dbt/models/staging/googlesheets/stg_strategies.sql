with source as (

    select * from {{ source('src_googlesheets', 'strategies') }}

),

renamed as (

    select
        TRIM(name) as strategy_name

    from source

)

select * from renamed
