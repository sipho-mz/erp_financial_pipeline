with source as (
    select * from delta_scan('{{var("ERP_SILVER_DIR", "data/silver")}}/outlays')
),

renamed as (
    select
        --- dates
        date                           as operations_date,

        --- categories
        account                         as account_name,
        line_item,

        --- measures
        consol_cost                     as consolidated_ops_cost,

        --- fiscal calender
        fiscal_year,

        -- pipeline metadata — always carry forward from Bronze
        _ingested_at,
        _source
    from source
)

select * from renamed