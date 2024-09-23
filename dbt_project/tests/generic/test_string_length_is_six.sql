{% test length_is_six(model, column_name) %}

with validation as (

    select
        {{ column_name }} as string_field

    from {{ model }}

),

validation_errors as (

    select
        string_field

    from validation
    -- if this is true, then even_field is actually odd!
    where length(string_field) != 6

)

select *
from validation_errors

{% endtest %}