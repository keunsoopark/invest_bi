with source as (

    select * from {{ source('src_googlesheets', 'balance_manual') }}

),

renamed as (

    select
        asset_name,
        asset_id,
        balance,
        currency,
        updated_at as date

    from source

)

select * from renamed
