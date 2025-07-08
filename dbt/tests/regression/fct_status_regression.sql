{{ 
  config(
    tags=['regression']
  ) 
}}

WITH snapshot AS (
    SELECT * 
    FROM {{ source('marts_facts_test_fixtures', 'fct_status_snapshot_2025_07_01') }}
),

current_status AS (
    SELECT * 
    FROM {{ ref('fct_status') }} 
    WHERE date = DATE('2025-07-01')
),

-- Add a surrogate join key that handles NULLs
snapshot_keys AS (
  SELECT *,
    CONCAT(
      COALESCE(CAST(date AS STRING), 'null'), '|',
      COALESCE(asset_name, 'null'), '|',
      COALESCE(strategy_name, 'null'), '|',
      COALESCE(strategy_details, 'null')
    ) AS join_key
  FROM snapshot
),

current_keys AS (
  SELECT *,
    CONCAT(
      COALESCE(CAST(date AS STRING), 'null'), '|',
      COALESCE(asset_name, 'null'), '|',
      COALESCE(strategy_name, 'null'), '|',
      COALESCE(strategy_details, 'null')
    ) AS join_key
  FROM current_status
)

SELECT *
FROM current_keys c
FULL OUTER JOIN snapshot_keys s
  ON c.join_key = s.join_key
WHERE
  c.purchase_amounts IS DISTINCT FROM s.purchase_amounts OR
  c.purchase_sum IS DISTINCT FROM s.purchase_sum OR
  c.balance IS DISTINCT FROM s.balance
