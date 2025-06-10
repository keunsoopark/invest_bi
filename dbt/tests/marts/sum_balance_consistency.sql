SELECT 'Sum mismatch between balance views' AS error_message
FROM (
  SELECT
    SUM(balance) AS sum_assets
  FROM {{ ref('status_by_assets') }}
) a
JOIN (
  SELECT
    SUM(balance) AS sum_groups
  FROM {{ ref('status_by_asset_groups') }}
) b
JOIN (
  SELECT
    SUM(balance) AS sum_strategies
  FROM {{ ref('status_by_strategies') }}
) c
WHERE NOT (
  ABS(a.sum_assets - b.sum_groups) < 0.01
  AND ABS(a.sum_assets - c.sum_strategies) < 0.01
)