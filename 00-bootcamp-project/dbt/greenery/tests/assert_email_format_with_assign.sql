select
    email
from {{ ref('stg_greenery__users') }}
where email not like '%@%.%'