"""
Stage 2.5 — VALIDATE (Quality Gate)

Runs Great Expectations validations against each Silver Delta table.
If any expectation fails, raises RuntimeError — the pipeline stops and
dbt never runs. This prevents corrupt or incomplete Silver data from
propagating into the Gold analytical layer.

Pipeline position:
  Ingest (Bronze) → Process (Silver) → [VALIDATE] → Transform (Gold)

Why an ephemeral context?
  GX offers two context types:
    - File-based: persists suites, checkpoints, and results to disk as YAML.
    - Ephemeral:  lives in memory for the duration of this process only.

  For a pipeline that will be orchestrated by Airflow (Phase 2), ephemeral
  is correct. Each DAG run gets a fresh context — no stale state from a
  previous run can interfere. File-based contexts are for interactive
  data exploration, not automated pipelines.
"""
import logging

import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToNotBeNull,
)
from deltalake import DeltaTable

from src.erp_financial_pipeline.config import SILVER_DIR

log = logging.getLogger(__name__)

# ── Expectation definitions ───────────────────────────────────────────
# Defined as module-level constants so they can be imported and
# tested independently in pytest without running the GX runtime.
#
# The * (spread/unpack) operator on COMMON_FISCAL_EXPECTATIONS means
# "insert these items inline into this list" — avoids repeating the
# same three expectations across every suite definition.

_COMMON_FISCAL = [
    ExpectColumnValuesToBeBetween(
        column="fiscal_year",
        min_value=2020,
        max_value=2030,
    )
]

# ── Core validation logic ─────────────────────────────────────────────

def _validate_table(
    context, #gx.AbstractDataContext,
    table_name: str,
    expectations: list,
) -> bool:
    """
    Validate one Silver Delta table against its expectation list.

    Wires up the GX object chain:
      DataSource → DataAsset → BatchDefinition → ValidationDefinition
    Then runs the validation and returns True if all expectations passed.
    """
    # Load the Silver Delta table — read the latest committed version
    df = DeltaTable(str(SILVER_DIR / table_name)).to_pandas()
    log.info("  Loaded %s rows from silver/%s", f"{len(df):,}", table_name)

    # ── Step 1: Data Source
    # Tells GX "my data comes from pandas DataFrames".
    # Other source types: SQL databases, Spark, cloud storage.
    data_source = context.data_sources.add_pandas(name=f"silver_{table_name}")

    # ── Step 2: Data Asset
    # A named, logical dataset within the source.
    # In production you might have one data source with many assets
    # (e.g., one Snowflake connection with dozens of table assets).
    data_asset = data_source.add_dataframe_asset(name=table_name)

    # ── Step 3: Batch Definition
    # Defines how to get one "batch" of data from the asset.
    # whole_dataframe = use the entire DataFrame as a single batch.
    # Other strategies: partition by date, by file, by partition key.
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"{table_name}_batch"
    )

    # ── Step 4: Expectation Suite
    # A named rulebook. Each expectation is one assertion about the data.
    suite = context.suites.add(
        gx.ExpectationSuite(name=f"{table_name}_suite")
    )
    for expectation in expectations:
        suite.add_expectation(expectation)

    # ── Step 5: Validation Definition
    # Permanently links a BatchDefinition to an ExpectationSuite.
    # Running it with a concrete DataFrame evaluates every expectation.
    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=f"validate_{table_name}",
            data=batch_definition,
            suite=suite,
        )
    )

    # ── Step 6: Run — pass the actual DataFrame here
    result = validation_def.run(
        batch_parameters={"dataframe": df}
    )

    # ── Step 7: Report results
    total   = len(result.results)
    passed  = sum(1 for r in result.results if r.success)
    failed  = total - passed

    if result.success:
        log.info(
            "  %s/%s expectations passed — silver/%s OK",
            passed, total, table_name,
        )
    else:
        log.error(
            "  %s/%s expectations FAILED for silver/%s",
            failed, total, table_name,
        )
        # Log every specific failure so the engineer knows exactly what broke
        for res in result.results:
            if not res.success:
                log.error(
                    "    ✗ %s on column '%s' → %s",
                    res.expectation_config.type,
                    res.expectation_config.kwargs.get("column", "table-level"),
                    res.result,
                )

    return result.success


# ── Entry point ───────────────────────────────────────────────────────

def run_validate() -> None:
    """
    Main entry point — validate every Silver table.

    Raises RuntimeError if any table fails. RuntimeError is caught by
    Airflow in Phase 2 and marks the task as FAILED, which prevents
    downstream tasks (dbt) from running.

    The pipeline MUST stop on a validation failure. Silent failures
    in data pipelines are the most dangerous kind — they produce
    analytically wrong reports without any visible error signal.
    """
    log.info("=" * 60)
    log.info("STAGE 2.5 — VALIDATE  (Quality Gate)")
    log.info("=" * 60)

    # One ephemeral context per pipeline run — fresh state every time
    context = gx.get_context()

    failures: list[str] = []

    if failures:
        raise RuntimeError(
            f"Quality gate FAILED for tables: {', '.join(failures)}. "
            "The Gold layer has not been updated. "
            "Investigate the errors above before retrying."
        )

    log.info("\nAll quality checks passed. Gold layer build authorised.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    run_validate()
