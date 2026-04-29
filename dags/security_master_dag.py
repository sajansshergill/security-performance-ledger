"""
security_master_dag.py

Daily security master refresh pipeline.

Schedule: 06:00 UTC (runs before markets open)
SLA: 90 minutes
Retries: 2 per task, 5-minute delay

Flow:
    fetch_openfigi
    fetch_refinitiv  ──► resolve_identifiers ──► load_to_snowflake
    fetch_bloomberg                                     │
                                                  dbt_run_staging
                                                        │
                                               dbt_run_security_master
                                                        │
                                               dbt_test_dim_security
                                                        │
                                               notify_on_completion
"""

from __future__ import annotations

import os
import logging
from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Local src imports (available via PYTHONPATH=/opt/airflow in Docker)
from src.ingestion.openfigi_client import OpenFIGIClient, SecurityIdentifier
from src.ingestion.refinitiv_client import RefinitivClient
from src.ingestion.bloomberg_client import BloombergClient
from src.mapping.identifier_resolver import IdentifierResolver
from src.loaders.snowflake_loader import SnowflakeLoader

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG-level config
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,  # swap for real alert hook
    "sla": timedelta(minutes=90),
}

# Sample CUSIP universe — in production, sourced from a reference file / DB table
CUSIP_UNIVERSE = [
    "037833100",  # Apple
    "594918104",  # Microsoft
    "023135106",  # Amazon
    "02079K305",  # Alphabet
    "67066G104",  # NVIDIA
    "46090E103",  # Visa
    "00724F101",  # AbbVie
    "808513105",  # Charles Schwab
    "69351T106",  # PNC Financial
    "172967424",  # Citigroup
]


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------


def fetch_openfigi(**context) -> None:
    batch_id = context["run_id"]
    client = OpenFIGIClient(api_key=os.getenv("OPENFIGI_API_KEY"))
    identifiers = [
        SecurityIdentifier(id_type="CUSIP", id_value=c) for c in CUSIP_UNIVERSE
    ]
    records = client.resolve_batch(identifiers, batch_id=batch_id)
    context["ti"].xcom_push(key="openfigi_records", value=records)
    log.info("fetch_openfigi: pushed %d records", len(records))


def fetch_refinitiv(**context) -> None:
    batch_id = context["run_id"]
    client = RefinitivClient()
    records = client.fetch_universe(CUSIP_UNIVERSE, batch_id=batch_id)
    context["ti"].xcom_push(key="refinitiv_records", value=records)
    log.info("fetch_refinitiv: pushed %d records", len(records))


def fetch_bloomberg(**context) -> None:
    batch_id = context["run_id"]
    client = BloombergClient()
    records = client.fetch_universe(CUSIP_UNIVERSE, batch_id=batch_id)
    context["ti"].xcom_push(key="bloomberg_records", value=records)
    log.info("fetch_bloomberg: pushed %d records", len(records))


def resolve_identifiers(**context) -> None:
    ti = context["ti"]
    batch_id = context["run_id"]

    openfigi_recs = (
        ti.xcom_pull(task_ids="fetch_openfigi", key="openfigi_records") or []
    )
    refinitiv_recs = (
        ti.xcom_pull(task_ids="fetch_refinitiv", key="refinitiv_records") or []
    )
    bloomberg_recs = (
        ti.xcom_pull(task_ids="fetch_bloomberg", key="bloomberg_records") or []
    )

    resolver = IdentifierResolver()
    resolver.ingest(openfigi_recs, source="openfigi", batch_id=batch_id)
    resolver.ingest(refinitiv_recs, source="refinitiv", batch_id=batch_id)
    resolver.ingest(bloomberg_recs, source="bloomberg", batch_id=batch_id)

    summary = resolver.summary()
    log.info("Resolver summary: %s", summary)

    ti.xcom_push(key="master_records", value=resolver.get_master_records())
    ti.xcom_push(key="conflict_records", value=resolver.get_conflict_records())
    ti.xcom_push(key="resolver_summary", value=summary)


def load_to_snowflake(**context) -> None:
    ti = context["ti"]

    master_records = (
        ti.xcom_pull(task_ids="resolve_identifiers", key="master_records") or []
    )
    conflict_records = (
        ti.xcom_pull(task_ids="resolve_identifiers", key="conflict_records") or []
    )
    openfigi_recs = (
        ti.xcom_pull(task_ids="fetch_openfigi", key="openfigi_records") or []
    )
    refinitiv_recs = (
        ti.xcom_pull(task_ids="fetch_refinitiv", key="refinitiv_records") or []
    )
    bloomberg_recs = (
        ti.xcom_pull(task_ids="fetch_bloomberg", key="bloomberg_records") or []
    )

    with SnowflakeLoader.from_env() as loader:
        if openfigi_recs:
            loader.upsert_dataframe(
                pd.DataFrame(openfigi_recs),
                table="RAW_SECURITIES_OPENFIGI",
                merge_keys=["_row_id"],
            )
        if refinitiv_recs:
            loader.upsert_dataframe(
                pd.DataFrame(refinitiv_recs),
                table="RAW_SECURITIES_REFINITIV",
                merge_keys=["_row_id"],
            )
        if bloomberg_recs:
            loader.upsert_dataframe(
                pd.DataFrame(bloomberg_recs),
                table="RAW_SECURITIES_BLOOMBERG",
                merge_keys=["_row_id"],
            )
        if master_records:
            loader.upsert_dataframe(
                pd.DataFrame(master_records),
                table="INT_SECURITY_MASTER_STAGING",
                merge_keys=["master_id"],
            )
        if conflict_records:
            loader.upsert_dataframe(
                pd.DataFrame(conflict_records),
                table="RAW_IDENTIFIER_CONFLICTS",
                merge_keys=["conflict_id"],
            )

    log.info(
        "load_to_snowflake: master=%d conflicts=%d",
        len(master_records),
        len(conflict_records),
    )


def run_dbt(command: str, select: str | None = None) -> callable:
    """Factory: returns a PythonOperator callable that runs a dbt CLI command."""

    def _run(**context):
        import subprocess

        cmd = [
            "dbt",
            command,
            "--project-dir",
            "/opt/airflow/dbt",
            "--profiles-dir",
            "/opt/airflow/dbt",
        ]
        if select:
            cmd += ["--select", select]
        result = subprocess.run(cmd, capture_output=True, text=True)
        log.info("dbt stdout:\n%s", result.stdout)
        if result.returncode != 0:
            log.error("dbt stderr:\n%s", result.stderr)
            raise RuntimeError(f"dbt {command} failed (rc={result.returncode})")

    return _run


def notify_on_completion(**context) -> None:
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="resolve_identifiers", key="resolver_summary") or {}
    log.info(
        "✅ security_master_dag complete | date=%s | securities=%s | conflicts=%s",
        context["ds"],
        summary.get("total_securities"),
        summary.get("total_conflicts"),
    )
    # Production: send to Slack / PagerDuty / Datadog event stream here


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="security_master_dag",
    default_args=DEFAULT_ARGS,
    description="Daily multi-source security master refresh",
    schedule_interval="0 6 * * 1-5",  # weekdays at 06:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["security-master", "finance", "daily"],
) as dag:

    t_openfigi = PythonOperator(
        task_id="fetch_openfigi", python_callable=fetch_openfigi
    )
    t_refinitiv = PythonOperator(
        task_id="fetch_refinitiv", python_callable=fetch_refinitiv
    )
    t_bloomberg = PythonOperator(
        task_id="fetch_bloomberg", python_callable=fetch_bloomberg
    )
    t_resolve = PythonOperator(
        task_id="resolve_identifiers", python_callable=resolve_identifiers
    )
    t_load = PythonOperator(
        task_id="load_to_snowflake", python_callable=load_to_snowflake
    )

    t_dbt_staging = PythonOperator(
        task_id="dbt_run_staging",
        python_callable=run_dbt("run", select="staging"),
    )
    t_dbt_master = PythonOperator(
        task_id="dbt_run_security_master",
        python_callable=run_dbt("run", select="int_security_master dim_security"),
    )
    t_dbt_test = PythonOperator(
        task_id="dbt_test_dim_security",
        python_callable=run_dbt("test", select="dim_security"),
    )
    t_notify = PythonOperator(
        task_id="notify_on_completion",
        python_callable=notify_on_completion,
    )

    # Fan-out ingestion, then fan-in to resolve → load → dbt
    [t_openfigi, t_refinitiv, t_bloomberg] >> t_resolve
    t_resolve >> t_load >> t_dbt_staging >> t_dbt_master >> t_dbt_test >> t_notify
