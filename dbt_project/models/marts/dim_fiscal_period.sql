/*
  dim_fiscal_period — The fiscal calendar dimension.

  One row per unique fiscal period (year / quarter / month).
  This is the shared time axis joined by every fact table.

  Why a surrogate key (period_id)?
  Natural keys like (fiscal_year=2024, fiscal_quarter=3, calendar_month=7)
  are verbose and fragile as join keys. A single integer surrogate key
  is faster to join, easier to read in query plans, and standard practice
  in dimensional modelling.
*/
with all_periods as (
    -- Union staging tables to capture every period in our data,
    -- regardless of which module it originated from.
    select distinct
        fiscal_year,
        date_trunc('month', cost_date)                              as period_start_date
    from {{ ref('stg_revenues') }}

    union

    select distinct 
        fiscal_year,
        date_trunc('month', operations_date)
    from {{ ref('stg_outlays') }}
),

enriched as (
    select
        -- Surrogate key: a stable integer that uniquely identifies this period
        row_number() over (order by period_start_date)              as period_id,

        period_start_date,
        fiscal_year,

        -- Human-readable labels for reports and dashboards
        'FY' || fiscal_year::varchar                                as fiscal_year_label,
        'FY' || fiscal_year::varchar || '-Q1'                       as fiscal_quarter_label,
        strftime(period_start_date, '%B %Y')                        as calendar_month_label
    from all_periods
)


select * from enriched