from src.mapping.identifier_resolver import IdentifierResolver


def test_resolver_deduplicates_by_cusip_and_prefers_bloomberg():
    resolver = IdentifierResolver()

    resolver.ingest(
        [
            {
                "cusip": "037833100",
                "isin": "US0378331000",
                "ticker": "AAPL",
                "security_name": "Apple Inc",
                "currency": "USD",
            }
        ],
        source="openfigi",
        batch_id="batch-1",
    )
    resolver.ingest(
        [
            {
                "cusip": "037833100",
                "isin": "US0378331000",
                "ticker": "AAPL US",
                "security_name": "APPLE INC",
                "currency": "USD",
            }
        ],
        source="bloomberg",
        batch_id="batch-1",
    )

    master_records = resolver.get_master_records()
    conflicts = resolver.get_conflict_records()

    assert len(master_records) == 1
    assert master_records[0]["ticker"] == "AAPL US"
    assert master_records[0]["source_of_truth"] == "bloomberg"
    assert {conflict["field_name"] for conflict in conflicts} == {
        "ticker",
        "security_name",
    }


def test_resolver_skips_records_without_canonical_identifier():
    resolver = IdentifierResolver()
    resolver.ingest([{"ticker": "MISSING"}], source="openfigi", batch_id="batch-1")

    assert resolver.get_master_records() == []
    assert resolver.summary()["total_securities"] == 0
