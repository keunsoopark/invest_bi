{{
  config(
    materialized='incremental',
    unique_key='date'
  )
}}


WITH

-- Step 1: Combine new source data with the last processed record from the destination table.
-- This "lookback" row provides the seed value to fill any gaps (like weekends) between runs.
-- On a full refresh, this CTE simply selects all data from the source.
source_with_lookback AS (
	SELECT
		date,
		usdnok,
		nokkrw
	FROM {{ source('src_external', 'fx') }}

	{% if is_incremental() %}
	-- In an incremental run, only process new records from the source.
	WHERE
		date > (
			SELECT MAX(date)
			FROM {{ this }}
		)
	UNION ALL
	-- Also include the most recent record that's already in our destination table.
	SELECT
		date,
		usdnok,
		nokkrw
	FROM {{ this }}
	WHERE
		date = (
			SELECT MAX(date)
			FROM {{ this }}
		)
	{% endif %}
),

-- Step 2: Generate a continuous date series covering only the new period we need to fill.
-- This is much more efficient than generating the full history every time.
date_series AS (
	SELECT
		calendar_date
	FROM
		UNNEST(
			GENERATE_DATE_ARRAY(
				(SELECT MIN(date)
				FROM source_with_lookback),
				(SELECT MAX(date)
				FROM source_with_lookback)
			)
		) AS calendar_date
),

-- Step 3: Join the limited data set onto the new date series.
-- This creates the NULLs for the new gaps we need to fill.
joined_data AS (
	SELECT
		d.calendar_date AS date,
		s.usdnok,
		s.nokkrw
	FROM date_series AS d
	LEFT JOIN source_with_lookback AS s ON d.calendar_date = s.date
),

-- Step 4: Apply the same forward-fill logic to this smaller, combined dataset.
filled_values AS (
	SELECT
		date,
		LAST_VALUE(usdnok IGNORE NULLS) OVER (
		ORDER BY
	date ROWS BETWEEN UNBOUNDED PRECEDING
	AND CURRENT ROW
		) AS usdnok,
		LAST_VALUE(nokkrw IGNORE NULLS) OVER (
		ORDER BY
	date ROWS BETWEEN UNBOUNDED PRECEDING
	AND CURRENT ROW
		) AS nokkrw
	FROM
		joined_data
)

-- Step 5: Final selection.
SELECT *
FROM filled_values

{% if is_incremental() %}
-- When running incrementally, we must filter the results to ONLY insert the new dates.
-- This prevents the "lookback" row from being inserted a second time.
WHERE
	date > (
		SELECT MAX(date)
		FROM {{ this }}
	)
{% endif %}
