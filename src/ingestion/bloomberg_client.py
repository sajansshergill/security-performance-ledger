"""Bloomberg Data License client stub."""

from __future__ import annotations

import hashlib

from src.utils.logger import get_logger

logger = get_logger(__name__)


class BloombergClient:
    """Return deterministic Bloomberg-like reference data records."""

    def fetch_universe(self, cusips: list[str], batch_id: str) -> list[dict]:
        records = [self._record(cusip.upper(), batch_id) for cusip in cusips]
        logger.info("Generated %d simulated Bloomberg records", len(records))
        return records

    @staticmethod
    def _record(cusip: str, batch_id: str) -> dict:
        digest = hashlib.sha256(cusip.encode()).hexdigest()
        ticker = {
            "037833100": "AAPL US",
            "594918104": "MSFT US",
            "023135106": "AMZN US",
            "02079K305": "GOOGL US",
            "67066G104": "NVDA US",
        }.get(cusip, f"BB{digest[:3].upper()} US")
        return {
            "_row_id": hashlib.sha256(
                f"bloomberg|{batch_id}|{cusip}".encode()
            ).hexdigest(),
            "batch_id": batch_id,
            "cusip": cusip,
            "isin": f"US{cusip}0",
            "figi": f"BBG{digest[:9].upper()}",
            "composite_figi": f"BBG{digest[9:18].upper()}",
            "ticker": ticker,
            "security_name": f"{ticker.split()[0]} COMMON STOCK",
            "security_type": "Common Stock",
            "market_sector": "Equity",
            "currency": "USD",
            "exchange_code": "US",
            "gics_sector": "Information Technology",
            "gics_industry": "Technology Hardware & Equipment",
            "country": "US",
            "asset_category": "Equity",
            "source": "bloomberg",
        }
