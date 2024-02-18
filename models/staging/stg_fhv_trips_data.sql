{{ config(materialized="view") }}

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    cast(SR_Flag as integer) as sr_flag,
    Affiliated_base_number as affiliated_base_number
      
from {{ source('staging', 'fhv_trips_data') }}
where extract(year from pickup_datetime) = 2019
and dispatching_base_num is not null

-- dbt build <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
