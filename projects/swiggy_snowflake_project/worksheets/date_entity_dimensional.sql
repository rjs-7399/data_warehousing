/*------------------------------------------------------------------------------
    DATE DIMENSION TABLE CREATION

    This script creates and populates a date dimension table for a data warehouse.
    The date dimension is a critical component in any dimensional model, enabling
    time-based analysis across all fact tables.

    The date range spans from the earliest order date in the system to the current date,
    providing a comprehensive time reference for all analytics.
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
    PART 2: DATE DIMENSION TABLE CREATION
------------------------------------------------------------------------------*/

-- Create the date dimension table in the consumption schema
-- This table will contain a complete set of calendar dates and their attributes
create or replace table consumption_sch.date_dim (
    DATE_DIM_HK NUMBER PRIMARY KEY comment 'Menu Dim HK (EDW)',     -- Surrogate key for date dimension
    CALENDAR_DATE DATE UNIQUE,                                      -- The actual calendar date
    YEAR NUMBER,                                                    -- Year component of the date
    QUARTER NUMBER,                                                 -- Quarter (1-4) within the year
    MONTH NUMBER,                                                   -- Month (1-12) within the year
    WEEK NUMBER,                                                    -- Week number within the year
    DAY_OF_YEAR NUMBER,                                             -- Day position within the year (1-365/366)
    DAY_OF_WEEK NUMBER,                                             -- Day position within the week (1-7)
    DAY_OF_THE_MONTH NUMBER,                                        -- Day position within the month (1-31)
    DAY_NAME STRING                                                 -- Name of the day (e.g., Monday, Tuesday)
)
comment = 'Date dimension table created using min of order data.';

/*------------------------------------------------------------------------------
    PART 3: DATE DIMENSION POPULATION WITH RECURSIVE CTE
------------------------------------------------------------------------------*/

-- Populate the date dimension using a recursive CTE approach
-- This generates all dates from the earliest order date to the current date
insert into consumption_sch.date_dim
with recursive my_date_dim_cte as
(
    -- Anchor member: Start with the current date and its attributes
    select
        current_date() as today,                -- Current date as starting point
        year(today) as year,                    -- Extract year from date
        quarter(today) as quarter,              -- Extract quarter from date
        month(today) as month,                  -- Extract month from date
        week(today) as week,                    -- Extract week number from date
        dayofyear(today) as day_of_year,        -- Extract day of year from date
        dayofweek(today) as day_of_week,        -- Extract day of week from date (1=Sunday, 7=Saturday)
        day(today) as day_of_the_month,         -- Extract day of month from date
        dayname(today) as day_name              -- Extract day name from date

    union all

    -- Recursive member: Subtract one day and calculate its attributes
    select
        dateadd('day', -1, today) as today_r,   -- Previous day (recursively)
        year(today_r) as year,                  -- Extract year from previous day
        quarter(today_r) as quarter,            -- Extract quarter from previous day
        month(today_r) as month,                -- Extract month from previous day
        week(today_r) as week,                  -- Extract week number from previous day
        dayofyear(today_r) as day_of_year,      -- Extract day of year from previous day
        dayofweek(today_r) as day_of_week,      -- Extract day of week from previous day
        day(today_r) as day_of_the_month,       -- Extract day of month from previous day
        dayname(today_r) as day_name            -- Extract day name from previous day
    from
        my_date_dim_cte
    where
        -- Terminate recursion when we reach the minimum order date
        today_r > (select date(min(order_date)) from clean_sch.orders)
)

-- Select from the CTE and apply hashing for the surrogate key
select
    hash(SHA1_hex(today)) as DATE_DIM_HK,       -- Create hash key based on date
    today as CALENDAR_DATE,                     -- The actual calendar date
    YEAR,                                       -- Year
    QUARTER,                                    -- Quarter (1-4)
    MONTH,                                      -- Month (1-12)
    WEEK,                                       -- Week of the year
    DAY_OF_YEAR,                                -- Day of the year (1-365/366)
    DAY_OF_WEEK,                                -- Day of the week (1-7)
    DAY_OF_THE_MONTH,                           -- Day of the month (1-31)
    DAY_NAME                                    -- Name of the day (e.g., Monday)
from my_date_dim_cte;

/*------------------------------------------------------------------------------
    Notes:

    1. The recursive CTE approach generates dates backward from the current date
       to the earliest order date in the system.

    2. The hash key is generated using SHA1_hex to ensure uniqueness.

    3. This date dimension can be extended with additional attributes like:
       - Is_Weekend
       - Is_Holiday
       - Fiscal periods
       - Month name
       - Season
       - Special business periods

    4. The date dimension should be refreshed periodically to include new dates
       as time progresses.
------------------------------------------------------------------------------*/