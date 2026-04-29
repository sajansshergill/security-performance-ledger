"""
Identifier Resolver — merges security records from OpenFIGI, Refinitiv,
and Bloomberg into a single deduplicated master record per unique security.

Resolution logic:
  - Canonical key: SHA-256(CUSIP) if present, else SHA-256(ISIN)
  - Source priority: bloomberg (1) > refinitiv (2) > openfigi (3)
  - Higher-priority source wins field-by-field; conflicts are logged
  - All conflicts are also returned as a separate list for Snowflake load
"""

import hashlib
import uuid
from dataclasses import dataclass, field
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)

SOURCE_PRIORITY: dict[str, int] = {
    "bloomberg": 1,
    "refinitiv": 2,
    "openfigi": 3,
}

# Fields that the resolver will attempt to reconcile across sources.
# Order matters: earlier fields are preferred as display label.
RECONCILED_FIELDS = [
    "ticker",
    "security_name",
    "security_type",
    "market_sector",
    "currency",
    "exchange_code",
    "figi",
    "composite_figi",
    "isin",
    "gics_sector",
    "gics_industry",
    "country",
    "asset_category",
]


@dataclass
class ResolvedSecurity:
    master_id: str  # SHA-256 of canonical CUSIP or ISIN
    cusip: Optional[str] = None
    isin: Optional[str] = None
    figi: Optional[str] = None
    composite_figi: Optional[str] = None
    ticker: Optional[str] = None
    security_name: Optional[str] = None
    security_type: Optional[str] = None
    market_sector: Optional[str] = None
    currency: Optional[str] = None
    exchange_code: Optional[str] = None
    gics_sector: Optional[str] = None
    gics_industry: Optional[str] = None
    country: Optional[str] = None
    asset_category: Optional[str] = None
    source_of_truth: str = "openfigi"
    sources_seen: list[str] = field(default_factory=list)
    # Pipe-delimited string; written to Snowflake as VARCHAR
    conflict_flags: str = ""


@dataclass
class ConflictRecord:
    conflict_id: str
    batch_id: str
    master_id: str
    cusip: Optional[str]
    isin: Optional[str]
    field_name: str
    value_winner: str
    source_winner: str
    value_loser: str
    source_loser: str


class IdentifierResolver:
    """
    Merge raw security records from multiple upstream sources into a
    unified master record per unique CUSIP / ISIN.

    Usage:
        resolver = IdentifierResolver()
        resolver.ingest(openfigi_records, source="openfigi", batch_id=batch_id)
        resolver.ingest(refinitiv_records, source="refinitiv", batch_id=batch_id)
        resolver.ingest(bloomberg_records, source="bloomberg", batch_id=batch_id)

        master = resolver.get_master_records()
        conflicts = resolver.get_conflict_records()
    """

    def __init__(self):
        self._master: dict[str, ResolvedSecurity] = {}
        self._conflicts: list[ConflictRecord] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(
        self, records: list[dict], source: str, batch_id: Optional[str] = None
    ) -> None:
        """
        Ingest a batch of raw records from a named source.
        Call once per source, in any order — priority handles resolution.
        """
        batch_id = batch_id or str(uuid.uuid4())
        incoming_priority = SOURCE_PRIORITY.get(source, 99)

        for rec in records:
            key = self._canonical_key(rec.get("cusip"), rec.get("isin"))
            if not key:
                logger.warning(
                    "Skipping record with no CUSIP/ISIN from %s: %s", source, rec
                )
                continue

            existing = self._master.get(key)

            if existing is None:
                self._master[key] = self._build_record(key, rec, source)
            else:
                self._merge(existing, rec, source, incoming_priority, batch_id)

        logger.info(
            "Ingested %d records from %s; master size=%d conflicts=%d",
            len(records),
            source,
            len(self._master),
            len(self._conflicts),
        )

    def get_master_records(self) -> list[dict]:
        """Return master records as dicts, ready for Snowflake MERGE."""
        return [vars(r) for r in self._master.values()]

    def get_conflict_records(self) -> list[dict]:
        """Return all detected conflicts as dicts for RAW_IDENTIFIER_CONFLICTS."""
        return [vars(c) for c in self._conflicts]

    def summary(self) -> dict:
        return {
            "total_securities": len(self._master),
            "total_conflicts": len(self._conflicts),
            "sources": list({r.source_of_truth for r in self._master.values()}),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _canonical_key(cusip: Optional[str], isin: Optional[str]) -> Optional[str]:
        """SHA-256 of CUSIP (preferred) or ISIN. Returns None if both absent."""
        raw = (cusip or isin or "").strip().upper()
        return hashlib.sha256(raw.encode()).hexdigest() if raw else None

    @staticmethod
    def _build_record(key: str, rec: dict, source: str) -> ResolvedSecurity:
        return ResolvedSecurity(
            master_id=key,
            cusip=rec.get("cusip"),
            isin=rec.get("isin"),
            figi=rec.get("figi"),
            composite_figi=rec.get("composite_figi") or rec.get("compositeFIGI"),
            ticker=rec.get("ticker"),
            security_name=rec.get("security_name") or rec.get("name"),
            security_type=rec.get("security_type") or rec.get("bb_yellow_key"),
            market_sector=rec.get("market_sector"),
            currency=rec.get("currency"),
            exchange_code=rec.get("exchange_code") or rec.get("exchange"),
            gics_sector=rec.get("gics_sector"),
            gics_industry=rec.get("gics_industry"),
            country=rec.get("country") or rec.get("country_of_issue"),
            asset_category=rec.get("asset_category"),
            source_of_truth=source,
            sources_seen=[source],
        )

    def _merge(
        self,
        existing: ResolvedSecurity,
        rec: dict,
        source: str,
        incoming_priority: int,
        batch_id: str,
    ) -> None:
        """Merge an incoming record into an existing master record."""
        existing_priority = SOURCE_PRIORITY.get(existing.source_of_truth, 99)
        if source not in existing.sources_seen:
            existing.sources_seen.append(source)

        # Normalised incoming values (handle field name variants per source)
        incoming: dict[str, Optional[str]] = {
            "ticker": rec.get("ticker"),
            "security_name": rec.get("security_name") or rec.get("name"),
            "security_type": rec.get("security_type") or rec.get("bb_yellow_key"),
            "market_sector": rec.get("market_sector"),
            "currency": rec.get("currency"),
            "exchange_code": rec.get("exchange_code") or rec.get("exchange"),
            "figi": rec.get("figi"),
            "composite_figi": rec.get("composite_figi") or rec.get("compositeFIGI"),
            "isin": rec.get("isin"),
            "gics_sector": rec.get("gics_sector"),
            "gics_industry": rec.get("gics_industry"),
            "country": rec.get("country") or rec.get("country_of_issue"),
            "asset_category": rec.get("asset_category"),
        }

        new_conflicts: list[str] = []

        for field_name in RECONCILED_FIELDS:
            new_val = incoming.get(field_name)
            old_val = getattr(existing, field_name, None)

            if not new_val:
                continue  # incoming has nothing useful; keep existing

            if old_val and new_val != old_val:
                # Conflict detected
                if incoming_priority <= existing_priority:
                    winner_src, winner_val = source, new_val
                    loser_src, loser_val = existing.source_of_truth, old_val
                else:
                    winner_src, winner_val = existing.source_of_truth, old_val
                    loser_src, loser_val = source, new_val

                conflict_raw = (
                    f"{field_name}:{loser_src}={loser_val}|{winner_src}={winner_val}"
                )
                new_conflicts.append(conflict_raw)

                self._conflicts.append(
                    ConflictRecord(
                        conflict_id=hashlib.sha256(
                            f"{existing.master_id}|{field_name}|{batch_id}".encode()
                        ).hexdigest(),
                        batch_id=batch_id,
                        master_id=existing.master_id,
                        cusip=existing.cusip,
                        isin=existing.isin,
                        field_name=field_name,
                        value_winner=winner_val,
                        source_winner=winner_src,
                        value_loser=loser_val,
                        source_loser=loser_src,
                    )
                )

            # Apply if incoming has higher (or equal) priority
            if not old_val or incoming_priority <= existing_priority:
                setattr(existing, field_name, new_val)

        if incoming_priority <= existing_priority:
            existing.source_of_truth = source

        if new_conflicts:
            existing_flags = (
                existing.conflict_flags.split("|") if existing.conflict_flags else []
            )
            existing.conflict_flags = "|".join(
                filter(None, existing_flags + new_conflicts)
            )
