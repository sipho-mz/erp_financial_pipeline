import pandas as pd
from deltalake import write_deltalake
from src.erp_financial_pipeline.config import SILVER_DIR

# Read Silver revenues
from deltalake import DeltaTable
df = DeltaTable(str(SILVER_DIR / "revenues")).to_pandas()

# Corrupt it: set all classifications to null
df["fiscal_year"] = 2056

# Overwrite Silver with the corrupted version
write_deltalake(str(SILVER_DIR / "revenues"), df, mode="overwrite", schema_mode="overwrite")

