with source as (
    select * from delta_scan('{{ var("ERP_SILVER_DIR", "data/silver") }}/cash_balance')
),

renamed as (
    select
        --- dates
        date                           as cash_balance_date,

        --- categories
        account                         as account_name,
        line_item,

        --- measures
        position_amt                     as position_amount,

        --- fiscal calender
        fiscal_year,

        -- pipeline metadata — always carry forward from Bronze
        _ingested_at,
        _source
    from source
)

select * from renamed
