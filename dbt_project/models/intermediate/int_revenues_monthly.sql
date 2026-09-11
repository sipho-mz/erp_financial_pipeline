/*
  int_revenue_monthly — Revenue aggregated to monthly grain per category.

  The staging table has one row per classification *line item* per month
  (a month may have multiple sub-classifications under one category).
  This model collapses them to one row per month per top-level category —
  the grain that fact tables and reports need.

  {{ ref('stg_revenues') }} is how dbt models reference other models.
  dbt reads all ref() calls, builds a dependency graph, and guarantees
  stg_revenues is created before this model runs. You never manage
  execution order manually.
*/
with revenues as (
    select * from {{ ref('stg_revenues' )}}
), 

monthly as (
    select 
        -- Truncate to month start — "2024-03-31" becomes "2024-03-01".
        -- All line items in March collapse to the same period_start_date.
        date_trunc('month', cost_date)               as period_start_date,

        fiscal_year,
        us_department_name,

        -- Aggregate across sub-classifications within each category
        sum(net_department_cost)                        as net_cost_month,
        count(*)                                        as department_count
    from revenues 
    group by
        date_trunc('month', cost_date),
        fiscal_year,
        us_department_name,
)

select * from monthly
