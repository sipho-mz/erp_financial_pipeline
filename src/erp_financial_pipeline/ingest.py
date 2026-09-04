"""
Stage 1 — INGEST (Bronze Layer)

Fetches raw financial data from the US Treasury Fiscal Data API and writes
it to the Bronze Delta Lake layer. Each API endpoint becomes one Delta table.

Design rules for Bronze:
  - Write exactly what the API returned — no transformations
  - Add ingestion metadata (_ingested_at, _source) for auditability
  - Overwrite the full table on each run (full refresh strategy)
  - ACID guarantees mean a failed run leaves the previous table intact
"""
import logging
import time

import pandas as pd
import requests
from deltalake import write_deltalake

from src.erp_financial_pipeline.config import (
    BASE_API_URL,
    BRONZE_DIR,
    ENDPOINTS,
    FISCAL_YEARS,
    PAGE_SIZE,
)

# Module-level logger — see explanation below why we do it this way
log = logging.getLogger(__name__)

# ── API layer ─────────────────────────────────────────────────────────

def fetch_page(endpoint: str, fiscal_year: str, page: int) -> dict:
    """
    Call one page of the Fiscal Data API for a given endpoint and fiscal year.
    Returns the full JSON payload (data + meta + links).
    """
    url = f"{BASE_API_URL}/{endpoint}"
    params = {
        "filters":      f"record_fiscal_year:eq:{fiscal_year}",
        "page[size]":   PAGE_SIZE,
        "page_number":  page,
        "sort":         "record_date",
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def fetch_all_pages(endpoint: str, fiscal_year: str) -> list[dict]:
    """
    Iterate through every page of results for one endpoint + fiscal year.
    Returns a flat list of all record dicts.
    """
    all_records: list[dict] = []
    page = 1

    while True:
        payload     = fetch_page(endpoint, fiscal_year, page)
        records     = payload["data"]
        total_pages = int(payload["meta"]["total-pages"])

        all_records.extend(records)
        log.info("  FY%s page %s/%s — %s records", fiscal_year, page, total_pages, len(records))

        if page >= total_pages:
            break

        page += 1
        time.sleep(0.5)

    return all_records

# ── Delta Lake writer ─────────────────────────────────────────────────

def write_to_bronze(records: list[dict], table_name: str) -> str:
    """
    Convert raw API records to a pandas DataFrame, attach ingestion metadata,
    and write to the Bronze Delta Lake table at BRONZE_DIR / table_name.

    Returns the string path to the Delta table (for logging).
    """
    df = pd.DataFrame(records)

    # Ingestion metadata — added in Bronze, preserved through all layers.
    # _ingested_at: when this record was loaded. Lets you answer
    #               "what did the pipeline see at 09:00 on Monday?"
    # _source:      which system produced this data. Critical once you
    #               have multiple sources feeding the same layer.
    df["_ingested_at"] = pd.Timestamp.now("UTC")
    df["_source"]      = "fiscaldata.treasury.gov"

    table_path = BRONZE_DIR / table_name
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    write_deltalake(
        table_path,        # path where the Delta table lives on disk
        df,                     # pandas DataFrame to write
        mode="overwrite",       # replace the whole table — full refresh
        schema_mode="overwrite"   # allow schema changes between API versions
    )

    return str(table_path)

# ── Entry point ───────────────────────────────────────────────────────

def run_ingest() -> None:
    """
    Main entry point. Loops over every configured endpoint and fiscal year,
    fetches all pages, and writes one Bronze Delta table per endpoint.
    """
    log.info("=" * 60)
    log.info("STAGE 1 - INGEST (Bronze Layer)")
    log.info("=" * 60)

    for source_name, cfg in ENDPOINTS.items():
        log.info("\nSource: %s -> endpoint: %s", source_name, cfg["endpoint"])
        all_records: list[dict] = []

        for fiscal_year in FISCAL_YEARS:
            log.info("  Fetching FY%s ...", fiscal_year)
            records = fetch_all_pages(cfg["endpoint"], fiscal_year)
            all_records.extend(records)

        table_path = write_to_bronze(all_records, cfg["table"])

        log.info("  Delta table written -> %s", table_path)
        log.info("  Total records: %s", f"{len(all_records):,}")

    log.info("\nIngest complete.")


if __name__ == "__main__":
    # basicConfig is ONLY called here, in the script entry point.
    # When Airflow calls run_ingest() in Phase 2, Airflow's own
    # logging infrastructure takes over — no conflict.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    run_ingest()
