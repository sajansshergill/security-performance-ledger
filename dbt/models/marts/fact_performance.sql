with positions as (
    select
        as_of_date,
        portfolio_id,
        sum(market_value_usd) as position_market_value_usd,
        sum(unrealized_gain_loss_usd) as unrealized_gain_loss_usd
    from {{ ref('fact_positions') }}
    group by 1, 2
),

nav as (
    select
        as_of_date,
        portfolio_id,
        nav_begin_usd,
        nav_end_usd,
        external_flow_usd,
        benchmark_return
    from {{ ref('stg_nav_events') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['n.portfolio_id', 'n.as_of_date']) }} as performance_key,
    n.as_of_date,
    n.portfolio_id,
    p.position_market_value_usd,
    p.unrealized_gain_loss_usd,
    n.nav_begin_usd,
    n.nav_end_usd,
    n.external_flow_usd,
    (n.nav_end_usd - n.nav_begin_usd - n.external_flow_usd) / nullif(n.nav_begin_usd, 0) as twr_daily,
    (n.nav_end_usd - n.nav_begin_usd) / nullif(n.nav_begin_usd + n.external_flow_usd, 0) as mwr_daily,
    n.benchmark_return,
    ((n.nav_end_usd - n.nav_begin_usd - n.external_flow_usd) / nullif(n.nav_begin_usd, 0)) - n.benchmark_return as active_return
from nav as n
left join positions as p
    on n.portfolio_id = p.portfolio_id
    and n.as_of_date = p.as_of_date
