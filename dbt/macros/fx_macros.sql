{% macro convert_to_nok(target_col, currency_col, fx_table_alias='f') %}
    case
        when {{ currency_col }} = 'NOK' then {{ target_col }}
        when {{ currency_col }} = 'USD' then {{ target_col }} * {{ fx_table_alias }}.usdnok
        when {{ currency_col }} = 'EUR' then {{ target_col }} * {{ fx_table_alias }}.eurnok
        when {{ currency_col }} = 'KRW' then {{ target_col }} / {{ fx_table_alias }}.nokkrw
        else null
    end
{% endmacro %}
