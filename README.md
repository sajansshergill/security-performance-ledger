# Multi-Source Security Master &amp; Performance Ledger

A production-grade financial data engineering pipeline that ingests securities data from multiple upstream sources(OpenFIGI, Refinitiv, Bloomberg), resolves complex financial identifiers (CUSIPs, ISINs, FIGIs, tickers), and builds a unified security master with dbt-modeled performance reporting —— orchestrated via Airflow and warehoused in Snowflake.

Build to reflect real-world data engineering practices at wealth managers, asset managers, and alternative data providers.

## Architecture
<img width="916" height="1316" alt="image" src="https://github.com/user-attachments/assets/08d52a07-57d2-4865-bae5-741f3d1eb75a" />

## Key Capabilities
**Identifier Management**
- Resolves CUSIPs, ISINs, FIGIs, composite FIGIs, and exchange tickers across three upstream sources
- Deterministic master_id via SHA-256 of canonical identifier, enabling idempotent upserts
- Conflict detection: logs field-levl disagreements between sources with source-of-truth assignment (Bloomberg > Refinitiv > OpenFIGI)

**Pipeline Reliability**
- Rate-limit aware OpenFIGI client (batch endpoint, 429 retry with exponential backoff)
- Snowflake MERGE-based upserts —— no full reloads, no duplicates
- Schema evolution: ALTER TABLE ADD COLUMN on new upstream fields without pipeline failure
- Airflow retry policies and SLA miss callbacks per task

**dbt Modeling**
- Staging models clean and cast raw source data
- int_secutiy_master resolves the unified golden record across sources
- dim_securiy —— Type 2 SCD-ready dimensional model
- fact_positions —— daily holdings snapshot with CUSIP/ISIN foreign keys
- fact_performance —— time-weighted return (TWR) and MWR calculations at portfolio and position level
- dbt tests: not-null, unique, referential integrity, and custom CUSIP format assetions

**Infrastructure**
- Docker Compose: Airflow (LocalExcecutor) + Postgres metadata DB
- Github Actions CI: dbt compile + test, pytest, flake8/black on eveny PR
- Terrform stubs for Snowflake warehouse + database provisioning

## Tech Stack
<img width="938" height="776" alt="image" src="https://github.com/user-attachments/assets/cbf30ffd-b1da-43a2-814e-9dd794f06e23" />

## Project Structure
<img width="1304" height="1330" alt="image" src="https://github.com/user-attachments/assets/b4c2e528-a3ec-4288-9180-dccc80e69880" />

## Getting Started
**Prerequisites**
- Docker & Docker Compose
- Python 3.11+
- Snowflake account
- OpenFIGI API key

**1. Clone and configure environment
**bashgit clone https://github.com/sajansshergill/security-master-ledger.git
cd security-master-ledger
cp .env.example .env

**Edit .env:**
envSNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=SECURITY_MASTER
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=SYSADMIN
OPENFIGI_API_KEY=your_key_here

**2. Provision Snowflake objects
**bash# Run DDL to create raw tables
snowsql -f sql/ddl/raw_securities.sql
snowsql -f sql/ddl/raw_positions.sql

Or use Terraform:
bashcd terraform
terraform init && terraform apply

**3. Start Airflow locally
**bashdocker-compose up --build -d
UI available at http://localhost:8080 (admin / admin)

**4. Install Python dependencies**
bashpython -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

**5. Configure dbt**
bashcd dbt
dbt deps
dbt debug   # verify Snowflake connection
dbt run     # build all models
dbt test    # run data quality tests

**6. Run tests**
bashpytest tests/ -v

## Data Model
**Raw Layer (Snowflake)**
<img width="1280" height="480" alt="image" src="https://github.com/user-attachments/assets/039cbf27-6968-41eb-85bc-0d9a71f9d99f" />

**dbt Mart Layer**
<ins>dim_security</ins> —— unified security master
Primary key: security_key (surrogate). Natural keys: cusip, isin, figi. Tracks source_of_truth, conflict_flags, SCD2 columns (valid_from, valid_to, is_current).
<ins>fact_positions</ins> —— daily holdings
Grain: portfolio_id x security_key x as_of_date. Includes market_value_usd, quantity, cost_basis.
<ins>fact_performance</ins> —— return calculations
Grain: portfolio_id x as_of_date. Included twr_daily, mwr_daily, benchmark_return, active_return.

## Airflow DAGs
**security_master_dag** (daily: 06:00 UTC)
fetch_openfigi → fetch_refinitiv → fetch_bloomberg
      └──────────────┬────────────────┘
               resolve_identifiers
                      │
               load_to_snowflake
                      │
                  dbt_run_staging
                      │
               dbt_run_security_master
                      │
               dbt_test_dim_security

performance_ledege_dag (daily, 07:30 UTC —— after security master completes)
ingest_positions → load_raw_positions → dbt_run_fact_positions
                                              │
ingest_nav_events → load_raw_perf → dbt_run_fact_performance
                                              │
                                     dbt_test_performance

## Financial Domain Coverage
This project demonstrates working knowledge of:
**- Securities identifiers:** CUSIP (9-char), ISIN (12-char ISO 6166), FIGI (12-char), composite FIGI, exchange ticker
**- Security types**: equities, fixed income, ETFs, mutual funds, derivatives (stubs)
**- Portfolio data**: positions, holdings, cost basis, market value
**- Performance reporting**: Time-Weightes Return (TWR), Money-Weighted-Return (MWR), benchmark attribution
**- Fund operations**: NAV events, corporate actions placeholders, custodian feed simulation

## Design Decisions
**Why SHA-256 as master_id?**
Deterministic across runs —— no sequence dependency, safe for distributed inserts, and enables idempotent MERGE without needing a database-generated key.

**Why MERGE over truncate-reload?**
Financial data pipelines require auditability. MERGE preserves the _loaded_at timestamp and conflict history on existing records while efficiently the ~5-15% daily change rate typical in secutiy master feeds.

**Why dbt over raw SQL stored procs?**
The engineer requires ability to read and revers-engineer stored procdures and legacy SQL. dbt models are then modern equivalent —— version-controlled, testable and lineage-tracked —— while the raw DDL and SQL layer in sql/ mirrors the stored-proc pattern for interoperability.

**Source priority (Bloomberg > Refinitiv > OpenFIGI**
Refelectes real-world data licensing hierarchy at institutional asset managers. Configurable via SOURCE_PRIORITY dict in identifier_reolver.py.

## Roadmap
✅ Corporate actions handler (splits, mergers, ticker changes)
✅ Full SCD Type 2 implementation on dim_security
✅ Datadog custom metrics on identifier conflict rate and pipeline SLA
✅ Great Expectations suite on raw ingestion layer
✅ Bloomberg Data License full integration




