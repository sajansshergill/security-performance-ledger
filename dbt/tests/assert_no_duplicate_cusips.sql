select
    cusip,
    count(*) as record_count
from {{ ref('dim_security') }}
where is_current
group by 1
having count(*) > 1
