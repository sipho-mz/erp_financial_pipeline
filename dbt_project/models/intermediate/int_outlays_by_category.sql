with outlays as (
    select * from {{ ref('stg_outlays') }}
),

monthly as (
    select
        date_trunc('month', operations_date)    as period_start_date,
        fiscal_year,
        account_name,
        line_item,
        sum(consolidated_ops_cost)              as consol_cost_amount,
        count(*)                                as account_name_count
    from outlays
    group by 
        date_trunc('month', operations_date),
        fiscal_year,
        account_name,
        line_item,
)

select * from monthly