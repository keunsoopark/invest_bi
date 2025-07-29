{% macro generate_date_series(start_date, end_date) %}
    UNNEST(
        GENERATE_DATE_ARRAY(
            {{ start_date }},
            {{ end_date }}
        )
    )
{% endmacro %}

{% macro last_value_ffill(column, order_by_col, partition_by=None, frame_clause="ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW") %}
    LAST_VALUE({{ column }} IGNORE NULLS) OVER (
        {%- if partition_by %}
            PARTITION BY {{ partition_by }}
        {%- endif %}
        ORDER BY {{ order_by_col }}
        {{ frame_clause}}
    )
{% endmacro %}
