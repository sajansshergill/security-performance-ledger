"""Simulated Refinitiv DSS REST client."""

from __future__ import annotations

import hashlib

from src.utils.logger import get_logger

logger = get_logger(__name__)


class RefinitivClient:
    """Return deterministic records that mirror a Refinitiv DSS extract."""

    def fetch_universe(self, cusips: list[str], batch_id: str) -> list[dict]:
        records = [self._record(cusip.upper(), batch_id) for cusip in cusips]
        logger.info("Generated %d simulated Refinitiv records", len(records))
        return records

    @staticmethod
    def _record(cusip: str, batch_id: str) -> dict:
        digest = hashlib.sha256(cusip.encode()).hexdigest()
        ticker = {
            "037833100": "AAPL.O",
            "594918104": "MSFT.O",
            "023135106": "AMZN.O",
            "02079K305": "GOOGL.O",
            "67066G104": "NVDA.O",
        }.get(cusip, f"R{digest[:4].upper()}.N")
        return {
            "_row_id": hashlib.sha256(
                f"refinitiv|{batch_id}|{cusip}".encode()
            ).hexdigest(),
            "batch_id": batch_id,
            "cusip": cusip,
            "isin": f"US{cusip}0",
            "ticker": ticker,
            "security_name": f"{ticker.split('.')[0]} ORD",
            "security_type": "Common Stock",
            "market_sector": "Equities",
            "currency": "USD",
            "exchange_code": ticker.split(".")[-1],
            "gics_sector": "Information Technology",
            "gics_industry": "Software & Services",
            "country": "US",
            "asset_category": "Equity",
            "source": "refinitiv",
        }
