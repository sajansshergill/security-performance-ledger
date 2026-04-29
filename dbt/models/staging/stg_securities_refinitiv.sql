select
    _row_id,
    batch_id,
    upper(cusip) as cusip,
    upper(isin) as isin,
    ticker,
    security_name,
    security_type,
    market_sector,
    currency,
    exchange_code,
    gics_sector,
    gics_industry,
    country,
    asset_category,
    source,
    _loaded_at
from {{ source('raw', 'raw_securities_refinitiv') }}
