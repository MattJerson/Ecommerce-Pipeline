
  
    

  create  table "postgres"."public"."mart_daily_revenue__dbt_tmp"
  
  
    as
  
  (
    -- models/marts/mart_daily_revenue.sql
-- Aggregates orders by day — the main KPI table for the dashboard

with orders as (
    select * from "postgres"."public"."stg_orders"
),

daily as (
    select
        ordered_at::date                            as order_date,
        category,
        count(distinct order_id)                    as total_orders,
        count(distinct user_id)                     as unique_customers,
        sum(quantity)                               as units_sold,
        round(sum(total_amount), 2)                 as gross_revenue,
        round(avg(total_amount), 2)                 as avg_order_value,
        round(avg(discount) * 100, 1)               as avg_discount_pct,

        -- breakdown by status
        count(*) filter (where status = 'delivered') as delivered_orders,
        count(*) filter (where status = 'cancelled') as cancelled_orders,

        -- breakdown by payment method
        count(*) filter (where payment_method = 'credit_card') as cc_orders,
        count(*) filter (where payment_method = 'gcash')       as gcash_orders,
        count(*) filter (where payment_method = 'paypal')      as paypal_orders

    from orders
    group by 1, 2
)

select * from daily
order by order_date desc, gross_revenue desc
  );
  