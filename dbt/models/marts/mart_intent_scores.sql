with signals as (
    select * from {{ ref('int_company_signals') }}
),

-- Use max filing date in dataset as recency anchor
date_anchor as (
    select max(file_date) as anchor_date
    from {{ ref('stg_sec_filings') }}
),

-- ICP filter: remove defense, aerospace, and government-contracting rows
-- Applied before scoring so excluded rows never influence aggregates
icp_filtered as (
    select s.*
    from signals s
    cross join date_anchor d
    where
        -- SIC exclusions: engineering/surveying services, national security, public admin
        coalesce(s.sic_code, '') not in ('8711', '8712', '8713', '9711', '9721', '9999')
        and not (s.sic_code >= '9100' and s.sic_code <= '9999')
        -- Agency exclusion: no Department of Defense awards (NULL = no award match, keep it)
        and (s.awarding_agency not ilike '%Department of Defense%' or s.awarding_agency is null)
),

scored as (
    select
        f.adsh,
        f.company_name,
        f.ticker,
        f.cik,
        f.file_date,
        f.biz_location,
        f.state_code,
        f.sic_code,
        f.federal_award_amount,
        f.award_id,
        f.awarding_agency,
        f.sector_avg_job_openings,
        f.mapped_industry,

        -- Award score (0–40): federal contract value
        case
            when f.federal_award_amount >= 10000000 then 40
            when f.federal_award_amount >= 1000000  then 25
            when f.federal_award_amount >= 100000   then 10
            else 0
        end as award_score,

        -- Recency score (0–30): days before dataset anchor date
        case
            when f.file_date >= dateadd('day', -30,  d.anchor_date) then 30
            when f.file_date >= dateadd('day', -90,  d.anchor_date) then 20
            when f.file_date >= dateadd('day', -180, d.anchor_date) then 10
            else 0
        end as recency_score,

        -- Sector demand score (0–30): BLS job openings (stored in thousands)
        case
            when f.sector_avg_job_openings >= 500 then 30
            when f.sector_avg_job_openings >= 100 then 20
            when f.sector_avg_job_openings >= 10  then 10
            else 0
        end as sector_demand_score,

        -- SaaS / software relevance boost (+10 for SIC 7370–7379)
        case
            when f.sic_code between '7370' and '7379' then 10
            else 0
        end as saas_boost

    from icp_filtered f
    cross join date_anchor d
),

row_scored as (
    select
        *,
        award_score + recency_score + sector_demand_score + saas_boost as row_intent_score
    from scored
),

-- Deduplicate: one row per company, aggregating across all filings and awards
deduped as (
    select
        company_name,

        -- Pick one representative value for company attributes
        min(ticker)                                 as ticker,
        min(cik)                                    as cik,
        min(state_code)                             as state_code,
        min(sic_code)                               as sic_code,
        min(biz_location)                           as biz_location,
        min(mapped_industry)                        as mapped_industry,

        -- Aggregate filing and award activity
        max(file_date)                              as most_recent_filing,
        sum(federal_award_amount)                   as total_award_amount,
        count(distinct nullif(award_id, ''))        as contract_count,
        count(distinct adsh)                        as filing_count,

        -- Carry the highest score this company achieved across its rows
        max(row_intent_score)                       as total_intent_score,

        -- Keep component scores from the highest-scoring row for transparency
        max(award_score)                            as award_score,
        max(recency_score)                          as recency_score,
        max(sector_demand_score)                    as sector_demand_score,
        max(saas_boost)                             as saas_boost,
        min(sector_avg_job_openings)                as sector_avg_job_openings

    from row_scored
    group by company_name
),

final as (
    select
        company_name,
        ticker,
        cik,
        state_code,
        sic_code,
        biz_location,
        mapped_industry,
        most_recent_filing,
        total_award_amount,
        contract_count,
        filing_count,
        total_intent_score,
        case
            when total_intent_score >= 80 then 'High'
            when total_intent_score >= 60 then 'Medium'
            else 'Low'
        end                         as intent_tier,
        award_score,
        recency_score,
        sector_demand_score,
        saas_boost,
        sector_avg_job_openings,
        current_timestamp()         as scored_at
    from deduped
)

select * from final
order by total_intent_score desc, total_award_amount desc
