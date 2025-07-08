{{ 
  config(
    materialized='incremental',
    unique_key=['date', 'asset_name']
  ) 
}}

-- Step 1: Load only new or changed rows from source
WITH source_data AS (
  SELECT
    date,
    asset_name,
    asset_id,
    price,
    currency
  FROM {{ ref('stg_asset_prices') }}
  {% if is_incremental() %}
  WHERE date > (SELECT MAX(date) FROM {{ this }})
  {% endif %}
),

-- Step 2: Combine with last known data (for carry-forward price fill)
seed_data AS (
  {% if is_incremental() %}
  SELECT *
  FROM {{ this }}
  WHERE date = (SELECT MAX(date) FROM {{ this }})
    AND asset_name NOT IN (
      SELECT asset_name FROM source_data
    )
  {% else %}
  SELECT * FROM {{ this }} WHERE FALSE  -- ensures empty result in full-refresh
  {% endif %}
),

combined_data AS (
  SELECT * FROM source_data
  UNION ALL
  SELECT * FROM seed_data
),

-- Step 3: Get asset metadata and start dates
distinct_assets AS (
  SELECT asset_name, asset_id, currency, MIN(date) AS initial_date
  FROM combined_data
  GROUP BY asset_name, asset_id, currency
),

-- Step 4: Build date range starting from initial date of each asset
date_series AS (
  SELECT calendar_date
  FROM UNNEST(GENERATE_DATE_ARRAY(
    (SELECT MIN(initial_date) FROM distinct_assets),
    CURRENT_DATE() - 1
  )) AS calendar_date
),

-- Step 5: Join to make a complete matrix of dates × assets
date_asset_matrix AS (
  SELECT
    d.calendar_date AS date,
    a.asset_name,
    a.asset_id,
    a.currency
  FROM date_series d
  CROSS JOIN distinct_assets a
  WHERE d.calendar_date >= a.initial_date
),

-- Step 6: Join price info (may be null)
joined_data AS (
  SELECT
    m.date,
    m.asset_name,
    m.asset_id,
    m.currency,
    s.price
  FROM date_asset_matrix m
  LEFT JOIN combined_data s
    ON m.date = s.date AND m.asset_name = s.asset_name
),

-- Step 7: Fill forward the last known price
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

-- Step 8: Final output
SELECT *
FROM filled_values

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
