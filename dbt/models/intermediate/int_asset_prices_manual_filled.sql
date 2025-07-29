{{
  config(
    materialized='incremental',
    unique_key=['date', 'asset_name']
  )
}}

WITH max_date_cte AS (
	SELECT MAX(date) AS max_date
	FROM {{ this }}
),

source_with_lookback AS (
  SELECT
    date,
    asset_name,
    asset_id,
    price,
    currency
  FROM {{ ref('stg_asset_prices_manual') }}

  {% if is_incremental() %}
  WHERE
    date > (SELECT max_date FROM max_date_cte)

  UNION ALL

  SELECT
    date,
    asset_name,
    asset_id,
    price,
    currency
  FROM {{ this }}
  WHERE
    date = (SELECT max_date FROM max_date_cte)
  {% endif %}
),

distinct_assets AS (
  SELECT asset_name, asset_id, currency, min(date) as initial_date
  FROM source_with_lookback
  group by asset_name, asset_id, currency
),

date_asset_matrix AS (
  SELECT
    a.asset_name,
    a.asset_id,
    a.currency,
    d AS date
  FROM distinct_assets a
  CROSS JOIN {{ generate_date_series(
        "a.initial_date",
        "CURRENT_DATE() - 1"
    ) }} as d
),

joined_data AS (
  SELECT
    m.date,
    m.asset_name,
    m.asset_id,
    m.currency,
    s.price
  FROM date_asset_matrix m
  LEFT JOIN source_with_lookback s
    ON m.date = s.date
    AND m.asset_name = s.asset_name
),

filled_values AS (
  SELECT
    date,
    asset_name,
    asset_id,
    {{ last_value_ffill('price', 'date', 'asset_name') }} AS price,
    currency
  FROM joined_data
)

SELECT *
FROM filled_values

{% if is_incremental() %}
WHERE date > (SELECT max_date FROM max_date_cte)
{% endif %}
