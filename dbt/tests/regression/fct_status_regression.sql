{{ 
  config(
    tags=['regression']
  ) 
}}

SELECT *
FROM {{ ref('fct_status') }}
WHERE date = "2005-07-01"
EXCEPT DISTINCT
SELECT *
FROM {{ source('marts_facts_test_fixtures', 'fct_status_snapshot_2025_07_01') }}
