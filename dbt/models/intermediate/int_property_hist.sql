{{
  config(
    materialized='table'
  )
}}

with property_hist as (
    select *
    from {{ ref('stg_property_hist') }}
),

property_hist_distributed_cost_base as (
    select *,
        EXTRACT(YEAR FROM date) * 12 + EXTRACT(MONTH FROM date) AS month_num,
    from {{ ref('stg_property_hist') }}
),

property_hist_distributed_cost as (
    select
        date,
        property_name,
        property_value,
        debt,
        tenant,
        monthly_rent,
        loan_payback,
        loan_cost,
        interest_rate,
        felleskostnad,
        LAST_VALUE(garbage_cost IGNORE NULLS) OVER (
            PARTITION BY property_name
            ORDER BY month_num
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) / 6 AS garbage_cost,
        LAST_VALUE(forsikring_cost IGNORE NULLS) OVER (
            PARTITION BY property_name
            ORDER BY month_num
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) / 12 AS forsikring_cost,
        LAST_VALUE(water_cost IGNORE NULLS) OVER (
            PARTITION BY property_name
            ORDER BY month_num
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) / 4 AS water_cost,
        electricity_cost,
        internet_cost,
        rent_mgmt_cost,
        depreciation_cost
    from property_hist_distributed_cost_base
),

property_hist_distributed_cost_not_null as (
    select 
        date,
        property_name,
        property_value,
        debt,
        tenant,
        monthly_rent,
        loan_payback,
        loan_cost,
        interest_rate,
        felleskostnad,
        COALESCE(garbage_cost, 0) AS garbage_cost,
        COALESCE(forsikring_cost, 0) AS forsikring_cost,
        COALESCE(water_cost, 0) AS water_cost,
        electricity_cost,
        internet_cost,
        rent_mgmt_cost,
        depreciation_cost
    from property_hist_distributed_cost
)

select * from property_hist_distributed_cost_not_null
