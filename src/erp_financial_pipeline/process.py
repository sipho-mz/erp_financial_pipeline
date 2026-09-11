"""
Stage 2 — PROCESS (Silver Layer)

Reads each Bronze Delta table, applies type casting and column renaming,
drops rows that violate minimum quality rules, and writes clean Delta
tables to the Silver layer.

Silver contract:
  - Every column has an explicit type (no raw strings for dates or amounts)
  - Column names follow our internal convention (no API-specific names)
  - Rows with null values in key fields are dropped
  - No aggregations, no joins, no business logic — that belongs in Gold
"""
import logging

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from src.erp_financial_pipeline.config import BRONZE_DIR, SILVER_DIR

log = logging.getLogger(__name__)

# ── Bronze reader ─────────────────────────────────────────────────────

def load_bronze_table(table_name: str) -> pd.DataFrame:
    """
    Read the latest committed version of a Bronze Delta table.
    Returns a pandas DataFrame containing every row and column,
    including the _ingested_at and _source metadata columns.
    """
    table_path =  BRONZE_DIR / table_name
    dt = DeltaTable(str(table_path))
    df = dt.to_pandas()
    log.info("  Loaded %s rows from bronze/%s", f"{len(df):,}", table_name)
    return df

# ── Shared helpers ────────────────────────────────────────────────────

def _clean_amount(series: pd.Series) -> pd.Series:
    """
    Convert an API amount column to float64.

    The Fiscal Data API returns monetary values as strings.
    Some may contain commas ("1,234,567.89"), some may be empty
    or null. errors='coerce' turns anything unparseable into NaN
    rather than raising an exception — safe for production code.
    """
    return(
        series.astype(str)
            .str.strip()
            .str.replace(",", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
    )

# ── Endpoint-specific processing functions ────────────────────────────
# Prefixed with _ to signal these are private to this module.
# Only run_process() should call them.

def _process_net_cost(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the Net Cost by agency table .
    """
    return (
        df
        .assign(
            date        = pd.to_datetime(df["record_date"], errors="coerce"),
            fiscal_year = pd.to_numeric(df["stmt_fiscal_year"], errors="coerce").astype("Int64"),
            agency_name = df["agency_nm"].str.strip(),
            net_cost = _clean_amount(df["net_cost_bil_amt"]),
        ) [[
            "_ingested_at", "_source", "date", "fiscal_year", "agency_name", "net_cost",
        ]]
        .dropna(subset=["date", "fiscal_year", "agency_name", "net_cost"])
        .drop_duplicates(subset=["agency_name"])
        .reset_index(drop=True)
    )

def _process_ops(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the Operations table .
    """
    return (
        df
        .assign(
            date         = pd.to_datetime(df["record_date"], errors="coerce"),
            fiscal_year  = pd.to_numeric(df["stmt_fiscal_year"], errors="coerce").astype("Int64"),
            account      = df["account_desc"].str.strip(),
            line_item    = df["line_item_desc"].str.strip(),
            consol_cost  = _clean_amount(df["consolidated_bil_amt"]),
        ) [[
            "_ingested_at", "_source", "date", "account", "line_item", "consol_cost", "fiscal_year",
        ]]
        .dropna(subset=["consol_cost", "account"])
        .drop_duplicates(subset=["line_item"])
        .reset_index(drop=True)
    )

def _process_cash_balance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the Cash Balance table .
    """
    return (
        df
        .assign(
            date         = pd.to_datetime(df["record_date"], errors="coerce"),
            fiscal_year  = pd.to_numeric(df["stmt_fiscal_year"], errors="coerce").astype("Int64"),
            account      = df["account_desc"].str.strip(),
            line_item    = df["line_item_desc"].str.strip(),
            position_amt = _clean_amount(df["position_bil_amt"]),
        ) [[
            "_ingested_at", "_source", "date", "account", "line_item", "position_amt", "fiscal_year",
        ]]
        .dropna(subset=["position_amt", "account"])
        .drop_duplicates(subset=["line_item"])
        .reset_index(drop=True)
    )

# ── Silver writer ─────────────────────────────────────────────────────

def write_to_silver(df: pd.DataFrame, table_name: str) -> str:
    """Write a cleaned DataFrame to the Silver Delta Lake layer."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    table_path = SILVER_DIR / table_name

    write_deltalake(
        str(table_path),
        data=df,
        mode="overwrite",
        schema_mode="overwrite",
    )
    return str(table_path)


# ── Entry point ───────────────────────────────────────────────────────

def run_process() -> None:
    """
    Main entry point — read all three Bronze tables, clean each one,
    write all three Silver tables.
    """
    log.info("=" * 60)
    log.info("STAGE 2 — PROCESS  (Silver Layer)")
    log.info("=" * 60)

    tasks = [
        ("revenues", _process_net_cost),
        ("outlays", _process_ops),
        ("cash_balance", _process_cash_balance),
    ]

    for table_name, process_fn in tasks:
        log.info("\nProcessing: %s", table_name)

        raw   = load_bronze_table(table_name)
        clean = process_fn(raw)

        dropped = len(raw) - len(clean)
        if dropped:
            log.warning("  Dropped %s rows that failed quality checks", dropped)

        path = write_to_silver(clean, table_name)
        log.info("  Silver table written → %s", path)
        log.info("  Rows in: %s  |  Rows out: %s", f"{len(raw):,}", f"{len(clean):,}")

    log.info("\nProcess complete.")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    run_process()
