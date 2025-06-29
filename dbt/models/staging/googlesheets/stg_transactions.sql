with source as (

    select * from {{ source('src_googlesheets', 'transactions') }}

),

renamed as (

    select
        date,
        asset_name,
        asset_id,
        price,
        currency,
        amounts,
        strategy_name,
        strategy_details,
        version as transaction_version

    from source

)

select * from renamed
