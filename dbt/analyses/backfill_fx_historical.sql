WITH

-- Step 1: Generate a continuous series of dates from the earliest to the latest date in the source data.
date_series AS (
  SELECT
    calendar_date
  FROM
    UNNEST(
      GENERATE_DATE_ARRAY(
        (SELECT MIN(date) FROM {{ source('src_external', 'fx') }}),
        (SELECT MAX(date) FROM {{ source('src_external', 'fx') }})
      )
    ) AS calendar_date
),

-- Step 2: Left join the original FX data onto the continuous date series. This will create NULLs for weekends and holidays.
joined_data AS (
  SELECT
    d.calendar_date AS date,
    fx.usdnok,
    fx.nokkrw
  FROM
    date_series AS d
  LEFT JOIN
    {{ source('src_external', 'fx') }} AS fx
    ON d.calendar_date = fx.date
)

-- Step 3: Use the LAST_VALUE window function to fill the NULLs with the last non-null value, ordered by date.
SELECT
  date,
  LAST_VALUE(usdnok IGNORE NULLS) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS usdnok,
  LAST_VALUE(nokkrw IGNORE NULLS) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nokkrw
FROM
  joined_data
ORDER BY
  date
