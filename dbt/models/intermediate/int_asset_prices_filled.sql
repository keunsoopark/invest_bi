{{ 
  config(
    materialized='incremental',
    unique_key=['date', 'asset_name']
  ) 
}}

-- Step 1: Load only new or changed rows from source
WITH max_date_cte AS (
	SELECT MAX(date) AS max_date
	FROM {{ this }}
),

source_data AS (
  SELECT
    date,
    asset_name,
    asset_id,
    price,
    currency
  FROM {{ ref('stg_asset_prices') }}

  {% if is_incremental() %}
  WHERE date > (SELECT max_date FROM max_date_cte)
  {% endif %}
),

-- Step 2: Combine with last known data (for carry-forward price fill)
seed_data AS (
  {% if is_incremental() %}
    SELECT *
    FROM {{ this }}
    WHERE date = (SELECT max_date FROM max_date_cte)
      -- Without this condition, the new price data would not be used if the price at running date already exists in {{ this }}.
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

-- Step 5: Join price info (may be null)
joined_data AS (
  SELECT
    m.date,
    m.asset_name,
    m.asset_id,
    m.currency,
    s.price
  FROM date_asset_matrix m
  LEFT JOIN combined_data s
    ON m.date = s.date
    AND m.asset_name = s.asset_name
),

-- Step 6: Fill forward the last known price
filled_values AS (
  SELECT
    date,
    asset_name,
    asset_id,
    {{ last_value_ffill('price', 'date', 'asset_name') }} AS price,
    currency
  FROM joined_data
)

-- Step 7: Final output
SELECT *
FROM filled_values

{% if is_incremental() %}
WHERE
  date > (SELECT max_date FROM max_date_cte)
{% endif %}
