/*
  stg_revenues — Staging model for federal revenue records.

  Source: Silver Delta table (revenues).
  Job:    Rename API field names to internal business names.
          No filtering, no aggregation, no logic.

  Naming convention used throughout this project:
    stg_  = staging model (view, reads from Silver)
    int_  = intermediate model (view, joins/aggregations)
    fct_  = fact table (stored table, the analytical output)
    dim_  = dimension table (stored table, descriptive attributes)
*/
with source as (
    -- delta_scan() is a DuckDB table function that reads a Delta Lake table
    -- directly from disk, honouring the transaction log. No data loading step
    -- required — DuckDB reads the committed Parquet files on the fly.
    select * from delta_scan('{{ var("ERP_SILVER_DIR", "data/silver")}}/revenues')
), 

renamed as (
    select
        --- dates
        date                           as cost_date,

        --- categories
        agency_name                     as us_department_name,

        --- measures
        net_cost                        as net_department_cost,

        --- fiscal calender
        fiscal_year                     as fiscal_year,

        -- pipeline metadata — always carry forward from Bronze
        _ingested_at,
        _source
    from source
)


select * from renamed


