{% test not_future_date(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} > current_date
{% endtest %}
