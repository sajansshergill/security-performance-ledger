select
    _row_id,
    batch_id,
    to_date(as_of_date) as as_of_date,
    portfolio_id,
    try_to_decimal(nav_begin_usd, 38, 2) as nav_begin_usd,
    try_to_decimal(nav_end_usd, 38, 2) as nav_end_usd,
    try_to_decimal(external_flow_usd, 38, 2) as external_flow_usd,
    try_to_decimal(benchmark_return, 18, 8) as benchmark_return,
    _loaded_at
from {{ source('raw', 'raw_nav_events') }}
