with source as (
    select * from {{ source('raw', 'bls_job_openings') }}
),

renamed as (
    select
        series_id,
        industry,
        year::int                                     as year,
        period,
        period_name,
        value::int                                    as job_openings,
        year::varchar || '-' || right(period, 2)      as year_month
    from source
)

select * from renamed
