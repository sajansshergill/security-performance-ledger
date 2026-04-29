with staged as (
    select
        master_id,
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
        gics_sector,
        gics_industry,
        country,
        asset_category,
        source_of_truth,
        sources_seen,
        conflict_flags,
        _loaded_at
    from {{ source('raw', 'int_security_master_staging') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by master_id
            order by _loaded_at desc
        ) as row_num
    from staged
)

select
    master_id,
    cusip,
    isin,
    figi,
    composite_figi,
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
    source_of_truth,
    sources_seen,
    conflict_flags,
    _loaded_at as valid_from
from ranked
where row_num = 1
