with property_stats as (
    select * from {{ ref('stg_property_stats') }}
),

property_stats_unpivoted as (
    SELECT *
    FROM property_stats
    UNPIVOT (
        value FOR item IN (
            monthly_price_change_percentage,
            yearly_price_change_percentage
        )
    )
)

select * from property_stats_unpivoted
