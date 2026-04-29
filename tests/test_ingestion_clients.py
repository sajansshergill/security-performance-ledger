from src.ingestion.bloomberg_client import BloombergClient
from src.ingestion.openfigi_client import OpenFIGIClient, SecurityIdentifier
from src.ingestion.refinitiv_client import RefinitivClient


def test_openfigi_simulated_records_are_deterministic():
    client = OpenFIGIClient(api_key=None)
    records = client.resolve_batch(
        [SecurityIdentifier("CUSIP", "037833100")], batch_id="run-1"
    )

    assert records[0]["cusip"] == "037833100"
    assert records[0]["ticker"] == "AAPL"
    assert records[0]["source"] == "openfigi"


def test_vendor_stubs_return_expected_universe_size():
    cusips = ["037833100", "594918104"]

    assert len(RefinitivClient().fetch_universe(cusips, batch_id="run-1")) == 2
    assert len(BloombergClient().fetch_universe(cusips, batch_id="run-1")) == 2
