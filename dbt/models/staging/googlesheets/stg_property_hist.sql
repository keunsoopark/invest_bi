with source as (

    select * from {{ source('src_googlesheets', 'property_hist') }}

),

renamed as (

    select
        date,
        TRIM(name) as property_name,
        value as property_value,
        debt,
        TRIM(tenant) as tenant,
        COALESCE(monthly_rent, 0) AS monthly_rent,
        loan_payback,
        loan_cost,
        interest_rate * 100 AS interest_rate,
        felleskostnad,
        garbage_cost,
        forsikring_cost,
        water_cost,
        COALESCE(electricity_cost, 0) AS electricity_cost,
        COALESCE(internet_cost, 0) AS internet_cost,
        COALESCE(rent_mgmt_cost, 0) AS rent_mgmt_cost,
        depreciation_cost

    from source

)

select * from renamed
