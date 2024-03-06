{{ config(materialized="view") }}

select
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }}
    as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }}
    as dropoff_locationid,
    cast(sr_flag as numeric) as sr_flag,
    cast(affiliated_base_number as string) as affiliated_base_number,
from {{ source("staging", "fhv") }}
where extract(year from CAST(pickup_datetime AS TIMESTAMP)) = 2019

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
