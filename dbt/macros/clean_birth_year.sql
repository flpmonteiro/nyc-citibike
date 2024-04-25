{% macro clean_birth_year(birth_year) %}
    case
        when regexp_contains({{ birth_year }}, r'^\d{4}$')
        then cast({{ birth_year }} as int64)
        when regexp_contains({{ birth_year }}, r'^\d{4}\.\d+$')
        then cast(cast({{ birth_year }} as float64) as int64)
        when regexp_contains({{ birth_year }}, r'^\\N$')
        then null
        else null
    end
{% endmacro %}
