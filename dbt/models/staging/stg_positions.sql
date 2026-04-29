select
    _row_id,
    batch_id,
    to_date(as_of_date) as as_of_date,
    portfolio_id,
    upper(cusip) as cusip,
    upper(isin) as isin,
    try_to_decimal(quantity, 38, 6) as quantity,
    try_to_decimal(market_price, 38, 6) as market_price,
    try_to_decimal(market_value_usd, 38, 2) as market_value_usd,
    try_to_decimal(cost_basis_usd, 38, 2) as cost_basis_usd,
    _loaded_at
from {{ source('raw', 'raw_positions') }}
