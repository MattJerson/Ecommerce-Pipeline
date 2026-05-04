-- models/marts/mart_traffic_funnel.sql
-- Joins clicks + orders to measure conversion by page and device

with clicks as (
    select * from {{ ref('stg_page_clicks') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

-- sessions that resulted in an order (same user, order within 30 min of click)
converted as (
    select
        c.session_id,
        c.user_id,
        c.page,
        c.device,
        c.referrer,
        c.clicked_at,
        o.order_id,
        o.total_amount
    from clicks c
    inner join orders o
        on  c.user_id  = o.user_id
        and o.ordered_at between c.clicked_at
                             and c.clicked_at + interval '30 minutes'
),

traffic_summary as (
    select
        c.page,
        c.device,
        c.referrer,
        c.clicked_at::date                          as visit_date,
        count(distinct c.session_id)                as total_sessions,
        count(distinct c.user_id)
            filter (where not c.is_anonymous)       as logged_in_users,
        count(distinct cv.session_id)               as converted_sessions,
        round(
            count(distinct cv.session_id)::numeric
            / nullif(count(distinct c.session_id), 0) * 100
        , 2)                                        as conversion_rate_pct,
        round(coalesce(sum(cv.total_amount), 0), 2) as revenue_from_sessions,
        round(avg(c.duration_secs), 0)              as avg_session_secs

    from clicks c
    left join converted cv using (session_id)
    group by 1, 2, 3, 4
)

select * from traffic_summary
order by visit_date desc, total_sessions desc