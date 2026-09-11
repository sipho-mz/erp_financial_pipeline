"""
run_pipeline.py — Execute all pipeline stages in sequence.

Stages:
  1. Ingest   — API → Bronze Delta Lake
  2. Process  — Bronze → Silver Delta Lake (typed, cleaned)
  2.5 Validate — Quality gate (stops pipeline if Silver is corrupt)
  3. Transform — Silver → Gold (dbt/DuckDB dimensional models)

In Phase 2 this file is replaced by an Airflow DAG that runs the same
functions as individual tasks with retry logic and scheduling.
"""
import logging
import os
import subprocess
import sys
from pathlib import Path

from src.erp_financial_pipeline.ingest   import run_ingest
from src.erp_financial_pipeline.process  import run_process
from src.erp_financial_pipeline.validate import run_validate

log = logging.getLogger(__name__)


def run_dbt() -> None:
    """
    Invoke dbt as a subprocess.

    dbt is a CLI tool — it does not expose a Python API for running models.
    subprocess.run() with check=True raises CalledProcessError if dbt
    exits with a non-zero code (i.e., any model or test fails), which
    propagates as a pipeline failure the same way RuntimeError does.
    """
    log.info("Running dbt ...")
    subprocess.run(
        [
            "dbt", "run",
            "--project-dir", "dbt_project",
            "--profiles-dir", "dbt_project",
        ],
        check=True,   # raises CalledProcessError on dbt failure
        text=True,
    )
    log.info("dbt run complete.")


def main() -> None:
    sep = "=" * 60

    log.info(sep)
    log.info("  ERP FINANCIAL DATA PIPELINE")
    log.info(sep)

    log.info("\n── Stage 1: Ingest (Bronze) ──────────────────────")
    run_ingest()

    log.info("\n── Stage 2: Process (Silver) ─────────────────────")
    run_process()

    log.info("\n── Stage 2.5: Validate (Quality Gate) ───────────")
    run_validate()            # raises RuntimeError and exits if Silver is bad

    log.info("\n── Stage 3: Transform (Gold via dbt) ────────────")
    run_dbt()

    log.info("\n%s", sep)
    log.info("  PIPELINE COMPLETE")
    log.info(sep)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    main()

