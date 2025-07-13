with source as (

    select * from {{ source('src_googlesheets', 'transactions') }}

),

renamed as (

    select
        date,
        TRIM(asset_name) as asset_name,
        TRIM(asset_id) as asset_id,
        price,
        currency,
        amounts,
        strategy_name,
        TRIM(strategy_details) as strategy_details,
        version as transaction_version

    from source

)

select * from renamed
