select
    _row_id,
    batch_id,
    upper(cusip) as cusip,
    upper(isin) as isin,
    figi,
    composite_figi,
    ticker,
    security_name,
    security_type,
    market_sector,
    currency,
    exchange_code,
    source,
    _loaded_at
from {{ source('raw', 'raw_securities_openfigi') }}
