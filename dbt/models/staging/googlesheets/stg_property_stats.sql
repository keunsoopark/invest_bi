with source as (

    select * from {{ source('src_googlesheets', 'property_stats') }}

),

renamed as (

    select
        month,
        monthly_price_change_percentage,
        yearly_price_change_percentage

    from source

)

select * from renamed
