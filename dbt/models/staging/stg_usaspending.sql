with source as (
    select * from {{ source('raw', 'usaspending_awards') }}
),

renamed as (
    select
        recipient_name,
        award_amount::float                       as award_amount,
        awarding_agency,
        state_code,
        award_id,
        start_date::date                          as start_date,
        naics_code
    from source
    where recipient_name is not null
)

select * from renamed
