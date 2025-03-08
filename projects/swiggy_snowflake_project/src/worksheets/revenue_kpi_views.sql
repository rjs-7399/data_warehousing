/*------------------------------------------------------------------------------
    REVENUE KPI VIEWS FOR BUSINESS ANALYTICS

    This script creates a suite of analytical views in the consumption layer
    to support business reporting and dashboards. These views provide revenue
    KPIs at different time granularities (yearly, monthly, daily) and with
    different dimensions (restaurant, day of week).

    All metrics are based on the order_item_fact table and leverage the
    date dimension for time-based analysis.
------------------------------------------------------------------------------*/

/*------------------------------------------------------------------------------
    PART 1: INITIAL SETUP AND CONTEXT
------------------------------------------------------------------------------*/

-- Set up the session context
use role sysadmin;
use warehouse adhoc_wh;
use database sandbox;
use schema stage_sch;

/*------------------------------------------------------------------------------
    PART 2: YEARLY REVENUE KPI VIEW
------------------------------------------------------------------------------*/

-- Create view for yearly revenue metrics
-- This view aggregates order data to show year-over-year performance
create or replace view consumption_sch.vw_yearly_revenue_kpis as
select
    d.year as year,                                                 -- Year dimension from date_dim
    sum(fact.subtotal) as total_revenue,                            -- Total revenue for the year
    count(distinct fact.order_id) as total_orders,                  -- Total number of orders
    round(sum(fact.subtotal) / count(distinct fact.order_id), 2)    -- Average revenue per order
        as avg_revenue_per_order,
    round(sum(fact.subtotal) / count(fact.order_item_id), 2)        -- Average revenue per item
        as avg_revenue_per_item,
    max(fact.subtotal) as max_order_value                           -- Maximum order value
from
    consumption_sch.order_item_fact fact
join
    consumption_sch.date_dim d
on
    fact.order_date_dim_key = d.date_dim_hk                         -- Join fact table with date dimension
where
    DELIVERY_STATUS = 'Delivered'                                   -- Only include delivered orders
group by
    d.year
order by
    d.year;

/*------------------------------------------------------------------------------
    PART 3: MONTHLY REVENUE KPI VIEW
------------------------------------------------------------------------------*/

-- Create view for monthly revenue metrics
-- This view provides month-by-month performance analysis
CREATE OR REPLACE VIEW consumption_sch.vw_monthly_revenue_kpis AS
SELECT
    d.YEAR AS year,                                                 -- Year dimension
    d.MONTH AS month,                                               -- Month dimension
    SUM(fact.subtotal) AS total_revenue,                            -- Total revenue for the month
    COUNT(DISTINCT fact.order_id) AS total_orders,                  -- Total number of orders
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2)    -- Average revenue per order
        AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2)        -- Average revenue per item
        AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value                           -- Maximum order value
FROM
    consumption_sch.order_item_fact fact
JOIN
    consumption_sch.DATE_DIM d
ON
    fact.order_date_dim_key = d.DATE_DIM_HK                         -- Join with date dimension
where
    DELIVERY_STATUS = 'Delivered'                                   -- Only include delivered orders
GROUP BY
    d.YEAR, d.MONTH
ORDER BY
    d.YEAR, d.MONTH;

/*------------------------------------------------------------------------------
    PART 4: DAILY REVENUE KPI VIEW
------------------------------------------------------------------------------*/

-- Create view for daily revenue metrics
-- This view enables day-by-day analysis of revenue performance
CREATE OR REPLACE VIEW consumption_sch.vw_daily_revenue_kpis AS
SELECT
    d.YEAR AS year,                                                 -- Year dimension
    d.MONTH AS month,                                               -- Month dimension
    d.DAY_OF_THE_MONTH AS day,                                      -- Day dimension
    SUM(fact.subtotal) AS total_revenue,                            -- Total revenue for the day
    COUNT(DISTINCT fact.order_id) AS total_orders,                  -- Total number of orders
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2)    -- Average revenue per order
        AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2)        -- Average revenue per item
        AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value                           -- Maximum order value
FROM
    consumption_sch.order_item_fact fact
JOIN
    consumption_sch.DATE_DIM d
ON
    fact.order_date_dim_key = d.DATE_DIM_HK                         -- Join with date dimension
where
    DELIVERY_STATUS = 'Delivered'                                   -- Only include delivered orders
GROUP BY
    d.YEAR, d.MONTH, d.DAY_OF_THE_MONTH                             -- Group by date components
ORDER BY
    d.YEAR, d.MONTH, d.DAY_OF_THE_MONTH;

/*------------------------------------------------------------------------------
    PART 5: DAY OF WEEK REVENUE KPI VIEW
------------------------------------------------------------------------------*/

-- Create view for day-of-week revenue metrics
-- This view enables analysis of performance patterns by day of week
CREATE OR REPLACE VIEW consumption_sch.vw_day_revenue_kpis AS
SELECT
    d.YEAR AS year,                                                 -- Year dimension
    d.MONTH AS month,                                               -- Month dimension
    d.DAY_NAME AS DAY_NAME,                                         -- Day of week name (e.g., Monday)
    SUM(fact.subtotal) AS total_revenue,                            -- Total revenue by day of week
    COUNT(DISTINCT fact.order_id) AS total_orders,                  -- Total number of orders
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2)    -- Average revenue per order
        AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2)        -- Average revenue per item
        AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value                           -- Maximum order value
FROM
    consumption_sch.order_item_fact fact
JOIN
    consumption_sch.DATE_DIM d
ON
    fact.order_date_dim_key = d.DATE_DIM_HK                         -- Join with date dimension
GROUP BY
    d.YEAR, d.MONTH, d.DAY_NAME                                     -- Group by year, month, day name
ORDER BY
    d.YEAR, d.MONTH, d.DAY_NAME;

/*------------------------------------------------------------------------------
    PART 6: MONTHLY REVENUE BY RESTAURANT KPI VIEW
------------------------------------------------------------------------------*/

-- Create view for monthly revenue metrics by restaurant
-- This view enables analysis of restaurant performance over time
CREATE OR REPLACE VIEW consumption_sch.vw_monthly_revenue_by_restaurant AS
SELECT
    d.YEAR AS year,                                                 -- Year dimension
    d.MONTH AS month,                                               -- Month dimension
    fact.DELIVERY_STATUS,                                           -- Order delivery status
    r.name as restaurant_name,                                      -- Restaurant name
    SUM(fact.subtotal) AS total_revenue,                            -- Total revenue for restaurant
    COUNT(DISTINCT fact.order_id) AS total_orders,                  -- Total number of orders
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2)    -- Average revenue per order
        AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2)        -- Average revenue per item
        AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value                           -- Maximum order value
FROM
    consumption_sch.order_item_fact fact
JOIN
    consumption_sch.DATE_DIM d
ON
    fact.order_date_dim_key = d.DATE_DIM_HK                         -- Join with date dimension
JOIN
    consumption_sch.restaurant_dim r
ON
    fact.restaurant_dim_key = r.RESTAURANT_HK                       -- Join with restaurant dimension
GROUP BY
    d.YEAR, d.MONTH, fact.DELIVERY_STATUS, restaurant_name
ORDER BY
    d.YEAR, d.MONTH;

/*------------------------------------------------------------------------------
    USAGE NOTES

    1. These views provide a comprehensive set of revenue KPIs at different
       time granularities to support business reporting needs.

    2. The yearly view is useful for strategic planning and year-over-year
       comparisons.

    3. The monthly view helps track seasonal patterns and month-to-month
       performance trends.

    4. The daily view enables detailed analysis of daily performance and
       can identify specific days with unusual activity.

    5. The day-of-week view helps identify patterns in customer behavior
       throughout the week (e.g., weekday vs weekend trends).

    6. The restaurant view allows comparison of performance across different
       restaurants over time.

    7. Most views filter for 'Delivered' orders to focus on completed sales,
       except for the restaurant view which includes delivery status as a
       dimension for additional analysis.
------------------------------------------------------------------------------*/