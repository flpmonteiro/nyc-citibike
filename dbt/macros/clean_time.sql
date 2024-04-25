{% macro clean_time(column_name) %}
    case
        when regexp_contains({{ column_name }}, r'^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$')
        then safe.parse_timestamp('%m/%d/%Y %H:%M', {{ column_name }})
        when regexp_contains({{ column_name }}, r'^\d{1,2}/\d{1,2}/\d{4} \d{2}:\d{2}:\d{2}$')
        then safe.parse_timestamp('%m/%d/%Y %H:%M:%S', {{ column_name }})
        when regexp_contains({{ column_name }}, r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
        then safe.parse_timestamp('%Y-%m-%d %H:%M:%S', {{ column_name }})
        when regexp_contains({{ column_name }}, r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$')
        then safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S', {{ column_name }})
        else null
    end
{% endmacro %}