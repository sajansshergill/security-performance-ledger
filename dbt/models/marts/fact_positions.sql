select
    {{ dbt_utils.generate_surrogate_key(['p.portfolio_id', 'p.as_of_date', 'coalesce(s.security_key, p.cusip)']) }} as position_key,
    p.as_of_date,
    p.portfolio_id,
    s.security_key,
    p.cusip,
    p.isin,
    p.quantity,
    p.market_price,
    p.market_value_usd,
    p.cost_basis_usd,
    p.market_value_usd - p.cost_basis_usd as unrealized_gain_loss_usd
from {{ ref('stg_positions') }} as p
left join {{ ref('dim_security') }} as s
    on p.cusip = s.cusip
    and s.is_current
