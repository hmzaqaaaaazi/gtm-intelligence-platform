with source as (
    select * from {{ source('raw', 'sec_8k_filings') }}
),

renamed as (
    select
        adsh,
        display_name_raw,
        company_name,
        ticker,
        cik,
        file_date::date    as file_date,
        biz_location,
        state_code,
        sic_code,
        parse_json(items)  as items
    from source
    where company_name is not null
)

select * from renamed
