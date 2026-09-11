# erp-financial-pipeline

A data pipeline that ingests US Treasury financial statement data and processes it through a Delta Lake medallion architecture. The Treasury API is used as a proxy for a real SAP FI/CO export — the kind of financial extract a data engineer works with at most organisations running ERP software.

Work in progress. Ingestion, processing, and dbt models are done. Integrating Great Expectations for data quality validation next, then Airflow and Docker.

---

## What it does

Pulls three financial statement streams from the [US Treasury Fiscal Data API](https://fiscaldata.treasury.gov/api/v1):

- Monthly revenues (MTS Table 4) — SAP FI revenue accounting equivalent
- Monthly outlays (MTS Table 5) — SAP FI expenditure/AP equivalent
- Daily cash balance (DTS Table 1) — SAP FI treasury equivalent

Each stream is written to a Bronze Delta Lake table (raw, ACID-compliant), cleaned and typed to Silver, then modelled into a DuckDB warehouse via dbt. The Gold layer has three models: `fct_revenue`, `fct_outlays`, and `dim_fiscal_period`.

---

## Status

- [x] Bronze layer — API ingestion to Delta Lake
- [x] Silver layer — type casting, renaming, null handling
- [x] Gold layer — dbt Core models (staging, intermediate, marts)
- [ ] Data quality gate — Great Expectations between Silver and Gold
- [ ] Orchestration — Apache Airflow DAG
- [ ] Deployment — Docker, docker-compose

---

## Stack

- **Storage** — Delta Lake via delta-rs (no Spark, no JVM)
- **SQL engine** — DuckDB with the delta extension
- **Transformation** — dbt Core + dbt-duckdb adapter
- **Data quality** — Great Expectations (in progress)
- **Orchestration** — Apache Airflow (planned)
- **Containerisation** — Docker (planned)
- **Testing** — pytest, pytest-cov
- **Linting** — Ruff

---

## Setup

```bash
git clone https://github.com/sipho-mz/erp-financial-pipeline.git
cd erp-financial-pipeline

python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Mac/Linux

pip install -e ".[dev]"
```

dbt needs two environment variables pointing at the Silver and Gold directories:  
I used a fallback path to not make a mess of my local machine

---

## Running the pipeline

Each stage can be run independently:

```bash
python -m src.erp_financial_pipeline.ingest
python -m src.erp_financial_pipeline.process
dbt run --project-dir dbt_project --profiles-dir dbt_project
dbt test --project-dir dbt_project --profiles-dir dbt_project
```

Run tests:

```bash
pytest tests/ -v -m "not integration"
```

---

## Project layout

```
src/erp_financial_pipeline/
  config.py        paths and API settings
  ingest.py        Stage 1: fetch API, write Bronze Delta tables
  process.py       Stage 2: clean and type, write Silver Delta tables
  validate.py      Stage 2.5: Great Expectations quality gate (in progress)

dbt_project/
  models/
    staging/       rename and cast Silver fields
    intermediate/  aggregate to monthly grain per category
    marts/         fct_revenue, fct_outlays, dim_fiscal_period

data/
  bronze/          raw Delta Lake tables
  silver/          cleaned Delta Lake tables
  gold/            DuckDB warehouse (warehouse.duckdb)

tests/
  conftest.py      shared fixtures
  test_pipeline.py unit and integration tests
```

---

## Design notes

Bronze and Silver are stored as Delta Lake tables rather than plain Parquet or CSV. The main reason is ACID compliance — a write either completes fully or doesn't happen, so a crashed run never leaves a half-written table. Delta also keeps a transaction log, which makes it straightforward to see what changed between runs and roll back if needed.

The dbt-duckdb adapter reads Silver Delta tables directly via DuckDB's `delta_scan()` function, so there's no separate loading step between Silver and Gold. DuckDB handles the SQL execution and writes the output models to a local `.duckdb` file.
