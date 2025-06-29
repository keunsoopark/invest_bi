{{
  config(
    materialized='incremental',
    unique_key=['date', 'asset_name']
  )
}}

WITH source_data AS (
  SELECT
    date,
    asset_name,
    asset_id,
    price,
    currency
  FROM {{ ref('stg_asset_prices') }}
),

new_rows AS (
  {% if is_incremental() %}
  SELECT s.*
  FROM source_data s
  LEFT JOIN {{ this }} t
    ON s.date = t.date AND s.asset_name = t.asset_name
  WHERE t.date IS NULL
  {% else %}
  SELECT * FROM source_data
  {% endif %}
),

date_series AS (
  SELECT calendar_date
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      (SELECT MIN(date) FROM new_rows),
      CURRENT_DATE() - 1
    )
  ) AS calendar_date
),

distinct_assets AS (
  SELECT asset_name, asset_id, currency, min(date) as initial_date
  FROM new_rows
  group by asset_name, asset_id, currency
),

date_asset_matrix AS (
  SELECT
    d.calendar_date AS date,
    a.asset_name,
    a.asset_id,
    a.currency
  FROM date_series d
  JOIN distinct_assets a
    ON d.calendar_date >= a.initial_date
),

joined_data AS (
  SELECT
    m.date,
    m.asset_name,
    m.asset_id,
    m.currency,
    s.price
  FROM date_asset_matrix m
  LEFT JOIN new_rows s
    ON m.date = s.date AND m.asset_name = s.asset_name
),

filled_values AS (
  SELECT
    date,
    asset_name,
    asset_id,
    LAST_VALUE(price IGNORE NULLS) OVER (
      PARTITION BY asset_name ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS price,
    currency
  FROM joined_data
)

SELECT *
FROM filled_values
