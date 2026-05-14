with properties as (
    select * from {{ ref('fct_properties') }}
),

properties_unpivoted as (
    SELECT *
    FROM properties
    UNPIVOT (
        value FOR item IN (
            property_value,
            debt,
            property_equity,
            debt_ratio,
            monthly_capital_gain,
            monthly_capital_gain_ratio,
            yearly_capital_gain,
            yearly_capital_gain_ratio,
            monthly_rent,
            loan_payback,
            loan_cost,
            loan_cashflow,
            interest_rate,
            felleskostnad,
            garbage_cost,
            forsikring_cost,
            water_cost,
            electricity_cost,
            internet_cost,
            rent_mgmt_cost,
            depreciation_cost,
            eiendomsskatt_bolig,
            total_cost,
            total_cash_outflow,
            rent_net_income,
            total_cashflow,
            rent_nominal_yield,
            rent_net_yield,
            rent_nominal_equity_yield,
            rent_net_equity_yield,
            transactions,
            total_capital_gain,
            total_debt_repayment,
            total_debt_repayment_ratio,
            total_acc_profit
        )
    )
)

select * from properties_unpivoted
