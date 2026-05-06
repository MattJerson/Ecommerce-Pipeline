-- models/staging/stg_orders.sql
-- Cleans and type-casts raw_orders into a reliable staging layer

with source as (
    select * from "postgres"."public"."raw_orders"
),

cleaned as (
    select
        order_id,
        user_id,
        product_id,
        product_name,
        category,
        unit_price::numeric(10,2)   as unit_price,
        quantity::int                as quantity,
        discount::numeric(5,2)       as discount,
        total_amount::numeric(10,2)  as total_amount,
        currency,
        lower(status)                as status,
        lower(payment_method)        as payment_method,
        lower(customer_email)        as customer_email,
        shipping_city,
        event_timestamp::timestamp   as ordered_at,
        ingested_at

    from source
    where order_id is not null        -- drop malformed rows
      and total_amount > 0            -- drop zero-value orders
)

select * from cleaned