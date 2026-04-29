"""
Daily position snapshot and performance ledger pipeline.

Runs after the security master refresh and builds the dbt mart models used for
holdings and portfolio performance reporting.
"""

from __future__ import annotations

import hashlib
import logging
import subprocess
from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from src.loaders.snowflake_loader import SnowflakeLoader

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "sla": timedelta(minutes=60),
}

SAMPLE_PORTFOLIOS = ["CORE_GROWTH", "INCOME_BALANCED"]
SAMPLE_CUSIPS = ["037833100", "594918104", "023135106", "02079K305", "67066G104"]


def ingest_positions(**context) -> None:
    as_of_date = context["ds"]
    batch_id = context["run_id"]
    records = []
    for portfolio_idx, portfolio_id in enumerate(SAMPLE_PORTFOLIOS, start=1):
        for security_idx, cusip in enumerate(SAMPLE_CUSIPS, start=1):
            quantity = 100 * portfolio_idx * security_idx
            price = 75 + (security_idx * 12.5)
            records.append(
                {
                    "_row_id": hashlib.sha256(
                        f"positions|{batch_id}|{portfolio_id}|{cusip}".encode()
                    ).hexdigest(),
                    "batch_id": batch_id,
                    "as_of_date": as_of_date,
                    "portfolio_id": portfolio_id,
                    "cusip": cusip,
                    "isin": f"US{cusip}0",
                    "quantity": quantity,
                    "market_price": price,
                    "market_value_usd": round(quantity * price, 2),
                    "cost_basis_usd": round(quantity * price * 0.92, 2),
                }
            )
    context["ti"].xcom_push(key="position_records", value=records)
    log.info("Generated %d position records", len(records))


def ingest_nav_events(**context) -> None:
    as_of_date = context["ds"]
    batch_id = context["run_id"]
    records = []
    for idx, portfolio_id in enumerate(SAMPLE_PORTFOLIOS, start=1):
        nav_begin = 1_000_000 * idx
        external_flow = 25_000 * idx
        nav_end = nav_begin + external_flow + (8_500 * idx)
        benchmark_return = 0.004 + (idx * 0.0005)
        records.append(
            {
                "_row_id": hashlib.sha256(
                    f"nav|{batch_id}|{portfolio_id}".encode()
                ).hexdigest(),
                "batch_id": batch_id,
                "as_of_date": as_of_date,
                "portfolio_id": portfolio_id,
                "nav_begin_usd": nav_begin,
                "nav_end_usd": nav_end,
                "external_flow_usd": external_flow,
                "benchmark_return": benchmark_return,
            }
        )
    context["ti"].xcom_push(key="nav_records", value=records)
    log.info("Generated %d NAV records", len(records))


def load_raw_positions(**context) -> None:
    records = (
        context["ti"].xcom_pull(task_ids="ingest_positions", key="position_records")
        or []
    )
    if not records:
        log.info("No position records to load")
        return
    with SnowflakeLoader.from_env() as loader:
        loader.upsert_dataframe(
            pd.DataFrame(records), table="RAW_POSITIONS", merge_keys=["_row_id"]
        )


def load_raw_performance(**context) -> None:
    records = (
        context["ti"].xcom_pull(task_ids="ingest_nav_events", key="nav_records") or []
    )
    if not records:
        log.info("No NAV records to load")
        return
    with SnowflakeLoader.from_env() as loader:
        loader.upsert_dataframe(
            pd.DataFrame(records), table="RAW_NAV_EVENTS", merge_keys=["_row_id"]
        )


def run_dbt(command: str, select: str | None = None) -> callable:
    def _run(**context):
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
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        log.info("dbt stdout:\n%s", result.stdout)
        if result.returncode != 0:
            log.error("dbt stderr:\n%s", result.stderr)
            raise RuntimeError(f"dbt {command} failed (rc={result.returncode})")

    return _run


with DAG(
    dag_id="performance_ledger_dag",
    default_args=DEFAULT_ARGS,
    description="Daily position snapshots and portfolio performance calculations",
    schedule_interval="30 7 * * 1-5",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["performance-ledger", "finance", "daily"],
) as dag:
    t_positions = PythonOperator(
        task_id="ingest_positions", python_callable=ingest_positions
    )
    t_nav = PythonOperator(
        task_id="ingest_nav_events", python_callable=ingest_nav_events
    )
    t_load_positions = PythonOperator(
        task_id="load_raw_positions", python_callable=load_raw_positions
    )
    t_load_perf = PythonOperator(
        task_id="load_raw_performance", python_callable=load_raw_performance
    )
    t_fact_positions = PythonOperator(
        task_id="dbt_run_fact_positions",
        python_callable=run_dbt("run", select="fact_positions"),
    )
    t_fact_performance = PythonOperator(
        task_id="dbt_run_fact_performance",
        python_callable=run_dbt("run", select="fact_performance"),
    )
    t_tests = PythonOperator(
        task_id="dbt_test_performance",
        python_callable=run_dbt("test", select="fact_positions fact_performance"),
    )

    t_positions >> t_load_positions >> t_fact_positions
    t_nav >> t_load_perf >> t_fact_performance
    [t_fact_positions, t_fact_performance] >> t_tests
