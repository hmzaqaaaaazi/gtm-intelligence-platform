with sec as (
    select * from {{ ref('stg_sec_filings') }}
),

usaspending as (
    select * from {{ ref('stg_usaspending') }}
),

bls as (
    select * from {{ ref('stg_bls_jobs') }}
),

-- Normalise company names: lowercase, trim whitespace, strip trailing punctuation
-- so "Apple Inc." matches "APPLE INC" and "Booz Allen Hamilton, Inc." matches "BOOZ ALLEN HAMILTON INC"
sec_clean as (
    select
        *,
        regexp_replace(lower(trim(company_name)), '[.,;]+$', '') as company_name_clean
    from sec
),

usa_clean as (
    select
        *,
        regexp_replace(lower(trim(recipient_name)), '[.,;]+$', '') as recipient_name_clean
    from usaspending
),

sec_with_awards as (
    select
        s.adsh,
        s.company_name,
        s.ticker,
        s.cik,
        s.file_date,
        s.biz_location,
        s.state_code,
        s.sic_code,
        coalesce(u.award_amount, 0)   as federal_award_amount,
        coalesce(u.award_id, '')      as award_id,
        u.awarding_agency
    from sec_clean s
    left join usa_clean u
        on s.company_name_clean = u.recipient_name_clean
),

bls_avg as (
    select
        industry,
        avg(job_openings) as avg_job_openings
    from bls
    group by 1
),

final as (
    select
        sa.adsh,
        sa.company_name,
        sa.ticker,
        sa.cik,
        sa.file_date,
        sa.biz_location,
        sa.state_code,
        sa.sic_code,
        sa.federal_award_amount,
        sa.award_id,
        sa.awarding_agency,
        coalesce(b.avg_job_openings, 0) as sector_avg_job_openings,
        case
            when sa.sic_code between '7370' and '7379' then 'Information Technology'
            when sa.sic_code between '6000' and '6999' then 'Finance and Insurance'
            when sa.sic_code between '7380' and '7389' then 'Professional and Business Services'
            else 'Total Nonfarm'
        end as mapped_industry
    from sec_with_awards sa
    left join bls_avg b
        on b.industry = case
            when sa.sic_code between '7370' and '7379' then 'Information Technology'
            when sa.sic_code between '6000' and '6999' then 'Finance and Insurance'
            when sa.sic_code between '7380' and '7389' then 'Professional and Business Services'
            else 'Total Nonfarm'
        end
)

select * from final
