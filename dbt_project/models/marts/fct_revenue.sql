/*
  fct_revenue — Federal revenue fact table.
  Grain: one row per fiscal period × revenue category.

  This table answers analytical questions like:
    "How did Individual Income Tax revenue trend across FY2023 and FY2024?"
    "Which revenue category had the largest YoY variance in Q2?"
    "What share of total revenue came from payroll taxes in FY2024?"
*/
with monthly_costs as (
    select * from {{ ref('int_revenues_monthly') }}
),

fiscal_periods as (
    select * from {{ ref('dim_fiscal_period') }}
),

joined as (
    select
        -- Foreign key to dim_fiscal_period
        fp.period_id,

        -- Date context — carried for convenience, avoids joins in queries
        mr.period_start_date,
        fp.fiscal_year_label,
        fp.fiscal_quarter_label,
        fp.calendar_month_label,
        mr.fiscal_year,

        -- Business attributes
        mr.us_department_name,

        -- Measures
        mr.net_cost_month                       as net_cost_current_month,

        mr.department_count
    from monthly_costs mr 
    left join fiscal_periods fp
        on mr.fiscal_year    = fp.fiscal_year
)

select * from joined 