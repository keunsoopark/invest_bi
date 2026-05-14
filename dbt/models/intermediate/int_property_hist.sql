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
        {{ last_value_ffill('garbage_cost', 'month_num', 'property_name', 'ROWS BETWEEN 5 PRECEDING AND CURRENT ROW') }} / 6 AS garbage_cost,
        {{ last_value_ffill('forsikring_cost', 'month_num', 'property_name', 'ROWS BETWEEN 11 PRECEDING AND CURRENT ROW') }} / 12 AS forsikring_cost,
        {{ last_value_ffill('water_cost', 'month_num', 'property_name', 'ROWS BETWEEN 3 PRECEDING AND CURRENT ROW') }} / 4 AS water_cost,
        electricity_cost,
        internet_cost,
        rent_mgmt_cost,
        depreciation_cost,
        {{ last_value_ffill('eiendomsskatt_bolig', 'month_num', 'property_name', 'ROWS BETWEEN 5 PRECEDING AND CURRENT ROW') }} / 6 AS eiendomsskatt_bolig
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
        depreciation_cost,
        COALESCE(eiendomsskatt_bolig, 0) AS eiendomsskatt_bolig
    from property_hist_distributed_cost
)

select * from property_hist_distributed_cost_not_null
