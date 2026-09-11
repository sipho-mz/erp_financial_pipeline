/*
  fct_outlays — Federal outlays (expenditure) fact table.
  Grain: one row per fiscal period × outlay category.
*/
with monthly_ops as (
    select * from {{ ref('int_outlays_by_category') }}
),

fiscal_periods as (
    select * from {{ ref('dim_fiscal_period') }}
),

joined as (
    select
        fp.period_id,
        mo.period_start_date,
        fp.fiscal_year_label,
        fp.fiscal_quarter_label,
        fp.calendar_month_label,
        mo.fiscal_year,
        mo.account_name,
        mo.consol_cost_amount                                               as consol_cost_current_month,
        mo.account_name_count
    from monthly_ops mo
    left join fiscal_periods fp
        on  mo.fiscal_year    = fp.fiscal_year
)

select * from joined
