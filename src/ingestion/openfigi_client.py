"""OpenFIGI batch mapping API client.

The client supports the real OpenFIGI mapping endpoint when an API key is
available, and falls back to deterministic sample records for local Airflow
and unit-test runs.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass
from typing import Iterable

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)

OPENFIGI_MAPPING_URL = "https://api.openfigi.com/v3/mapping"


@dataclass(frozen=True)
class SecurityIdentifier:
    id_type: str
    id_value: str


class OpenFIGIClient:
    def __init__(
        self, api_key: str | None = None, base_url: str = OPENFIGI_MAPPING_URL
    ):
        self.api_key = api_key
        self.base_url = base_url

    def resolve_batch(
        self, identifiers: Iterable[SecurityIdentifier], batch_id: str
    ) -> list[dict]:
        identifiers = list(identifiers)
        if not self.api_key:
            logger.info("OPENFIGI_API_KEY not set; using simulated OpenFIGI records")
            return [
                self._simulate_record(identifier, batch_id)
                for identifier in identifiers
            ]

        records: list[dict] = []
        headers = {
            "Content-Type": "application/json",
            "X-OPENFIGI-APIKEY": self.api_key,
        }
        for start in range(0, len(identifiers), 100):
            chunk = identifiers[start : start + 100]
            payload = [
                {"idType": item.id_type, "idValue": item.id_value} for item in chunk
            ]
            response = self._post_with_retry(payload, headers)
            records.extend(self._normalise_response(chunk, response, batch_id))
        return records

    def _post_with_retry(
        self, payload: list[dict], headers: dict[str, str]
    ) -> list[dict]:
        for attempt in range(4):
            response = requests.post(
                self.base_url, json=payload, headers=headers, timeout=30
            )
            if response.status_code != 429:
                response.raise_for_status()
                return response.json()
            sleep_seconds = 2**attempt
            logger.warning("OpenFIGI rate limited; sleeping %s seconds", sleep_seconds)
            time.sleep(sleep_seconds)
        response.raise_for_status()
        return []

    @staticmethod
    def _normalise_response(
        identifiers: list[SecurityIdentifier],
        response_rows: list[dict],
        batch_id: str,
    ) -> list[dict]:
        records: list[dict] = []
        for identifier, response_row in zip(identifiers, response_rows):
            data = (response_row.get("data") or [{}])[0]
            records.append(
                {
                    "_row_id": str(uuid.uuid4()),
                    "batch_id": batch_id,
                    "cusip": (
                        identifier.id_value
                        if identifier.id_type.upper() == "CUSIP"
                        else None
                    ),
                    "isin": (
                        data.get("idValue")
                        if identifier.id_type.upper() == "ID_ISIN"
                        else None
                    ),
                    "figi": data.get("figi"),
                    "composite_figi": data.get("compositeFIGI"),
                    "ticker": data.get("ticker"),
                    "security_name": data.get("name"),
                    "security_type": data.get("securityType"),
                    "market_sector": data.get("marketSector"),
                    "currency": data.get("currency"),
                    "exchange_code": data.get("exchCode"),
                    "source": "openfigi",
                }
            )
        return records

    @staticmethod
    def _simulate_record(identifier: SecurityIdentifier, batch_id: str) -> dict:
        cusip = identifier.id_value.upper()
        digest = hashlib.sha256(cusip.encode()).hexdigest()
        ticker = {
            "037833100": "AAPL",
            "594918104": "MSFT",
            "023135106": "AMZN",
            "02079K305": "GOOGL",
            "67066G104": "NVDA",
        }.get(cusip, f"SEC{digest[:3].upper()}")
        return {
            "_row_id": hashlib.sha256(
                f"openfigi|{batch_id}|{cusip}".encode()
            ).hexdigest(),
            "batch_id": batch_id,
            "cusip": cusip,
            "isin": f"US{cusip}0",
            "figi": f"BBG{digest[:9].upper()}",
            "composite_figi": f"BBG{digest[9:18].upper()}",
            "ticker": ticker,
            "security_name": f"{ticker} Common Stock",
            "security_type": "Equity",
            "market_sector": "Equity",
            "currency": "USD",
            "exchange_code": "US",
            "source": "openfigi",
        }
