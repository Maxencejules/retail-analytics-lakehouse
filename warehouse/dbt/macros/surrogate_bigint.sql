{% macro surrogate_bigint(value_expression) -%}
abs(('x' || substr(md5({{ value_expression }}), 1, 16))::bit(64)::bigint)
{%- endmacro %}
