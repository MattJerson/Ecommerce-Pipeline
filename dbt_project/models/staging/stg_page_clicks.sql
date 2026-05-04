-- models/staging/stg_page_clicks.sql
-- Cleans raw_page_clicks, flags anonymous sessions

with source as (
    select * from {{ source('public', 'raw_page_clicks') }}
),

cleaned as (
    select
        session_id,
        user_id,
        case
            when user_id is null then true
            else false
        end                          as is_anonymous,
        page,
        coalesce(referrer, 'direct') as referrer,   -- null referrer = direct visit
        lower(device)                as device,
        upper(country)               as country,
        duration_secs::int           as duration_secs,
        event_timestamp::timestamp   as clicked_at,
        ingested_at

    from source
    where session_id is not null
)

select * from cleaned