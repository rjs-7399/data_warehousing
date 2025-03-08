/*------------------------------------------------------------------------------
    RESTAURANT DATA WAREHOUSE ETL WORKFLOW

    This script implements a complete ETL process for restaurant data in Snowflake,
    following a multi-layer data warehouse architecture:

    - Stage Layer: Raw data landing from external sources
    - Clean Layer: Data with proper types and business rules applied
    - Consumption Layer: Dimensional model with SCD Type 2 implementation

    The script handles initial load and delta processing with error handling.
------------------------------------------------------------------------------*/

/*------------------------------------------------------------------------------
    PART 1: INITIAL SETUP AND CONTEXT
------------------------------------------------------------------------------*/

-- Set up the session context
use schema stage_sch;
use warehouse adhoc_wh;
use database sandbox;

/*------------------------------------------------------------------------------
    PART 2: STAGE LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Create Restaurant table under stage schema with all text values
-- This approach ensures successful data loading without type conversion errors
create or replace table stage_sch.restaurant (
    restaurantid text,
    name text ,                                         -- restaurant name, required field
    cuisinetype text,                                    -- type of cuisine offered
    pricing_for_2 text,                                  -- pricing for two people as text
    restaurant_phone text WITH TAG (common.pii_policy_tag = 'SENSITIVE'),   -- phone number as text
    operatinghours text,                                 -- restaurant operating hours
    locationid text ,                                    -- location id, default as text
    activeflag text ,                                    -- active status
    openstatus text ,                                    -- open status
    locality text,                                       -- locality as text
    restaurant_address text,                             -- address as text
    latitude text,                                       -- latitude as text for precision
    longitude text,                                      -- longitude as text for precision
    createddate text,                                    -- record creation date
    modifieddate text,                                   -- last modified date

    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'Restaurant entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- Create append-only stream on restaurant table to capture data changes
-- This enables CDC (Change Data Capture) for downstream processing
create or replace stream stage_sch.restaurant_stm
on table stage_sch.restaurant
append_only = true
comment = 'This is the append-only stream object on restaurant table that only gets delta data';

/*------------------------------------------------------------------------------
    PART 3: CLEAN LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Create Restaurant table in clean schema with proper data types
-- This represents the validated and typed version of restaurant data
create or replace table clean_sch.restaurant (
    restaurant_sk number autoincrement primary key,              -- primary key with auto-increment
    restaurant_id number unique,                                 -- restaurant id without auto-increment
    name string(100) not null,                                   -- restaurant name, required field
    cuisine_type string,                                         -- type of cuisine offered
    pricing_for_two number(10, 2),                               -- pricing for two people, up to 10 digits with 2 decimal places
    restaurant_phone string(15) WITH TAG (common.pii_policy_tag = 'SENSITIVE'),     -- phone number, supports 10-digit or international format
    operating_hours string(100),                                  -- restaurant operating hours
    location_id_fk number,                                       -- reference id for location, defaulted to 1
    active_flag string(10),                                      -- indicates if the restaurant is active
    open_status string(10),                                      -- indicates if the restaurant is currently open
    locality string(100),                                        -- locality of the restaurant
    restaurant_address string,                                   -- address of the restaurant, supports longer text
    latitude number(9, 6),                                       -- latitude with 6 decimal places for precision
    longitude number(9, 6),                                      -- longitude with 6 decimal places for precision
    created_dt timestamp_tz,                                     -- record creation date
    modified_dt timestamp_tz,                                    -- last modified date, allows null if not modified

    -- additional audit columns
    _stg_file_name string,                                       -- file name for audit
    _stg_file_load_ts timestamp_ntz,                             -- file load timestamp for audit
    _stg_file_md5 string,                                        -- md5 hash for file content for audit
    _copy_data_ts timestamp_ntz default current_timestamp        -- timestamp when data is copied, defaults to current timestamp
)
comment = 'Restaurant entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- Create standard stream on clean restaurant table to capture all DML operations
-- This enables comprehensive change tracking for the dimensional model
create or replace stream clean_sch.restaurant_stm
on table clean_sch.restaurant
comment = 'This is a standard stream object on the clean restaurant table to track insert, update, and delete changes';

/*------------------------------------------------------------------------------
    PART 4: CONSUMPTION LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Create Restaurant dimension table in consumption schema
-- Implements SCD Type 2 pattern for historical tracking
CREATE OR REPLACE TABLE CONSUMPTION_SCH.RESTAURANT_DIM (
    RESTAURANT_HK NUMBER primary key,       -- Hash key for the restaurant location
    RESTAURANT_ID NUMBER,                   -- Restaurant ID without auto-increment
    NAME STRING(100),                       -- Restaurant name
    CUISINE_TYPE STRING,                    -- Type of cuisine offered
    PRICING_FOR_TWO NUMBER(10, 2),          -- Pricing for two people
    RESTAURANT_PHONE STRING(15) WITH TAG (common.pii_policy_tag = 'SENSITIVE'),            -- Restaurant phone number
    OPERATING_HOURS STRING(100),            -- Restaurant operating hours
    LOCATION_ID_FK NUMBER,                  -- Foreign key reference to location
    ACTIVE_FLAG STRING(10),                 -- Indicates if the restaurant is active
    OPEN_STATUS STRING(10),                 -- Indicates if the restaurant is currently open
    LOCALITY STRING(100),                   -- Locality of the restaurant
    RESTAURANT_ADDRESS STRING,              -- Full address of the restaurant
    LATITUDE NUMBER(9, 6),                  -- Latitude for the restaurant's location
    LONGITUDE NUMBER(9, 6),                 -- Longitude for the restaurant's location
    EFF_START_DATE TIMESTAMP_TZ,            -- Effective start date for the record
    EFF_END_DATE TIMESTAMP_TZ,              -- Effective end date for the record (NULL if active)
    IS_CURRENT BOOLEAN                      -- Indicates whether the record is the current version
)
COMMENT = 'Dimensional table for Restaurant entity with hash keys and SCD enabled.';

/*------------------------------------------------------------------------------
    PART 5: INITIAL DATA LOADING
------------------------------------------------------------------------------*/

-- Load initial restaurant data from CSV files into stage layer
-- Uses COPY command with error handling for efficient data loading
copy into stage_sch.restaurant(
    restaurantid,
    name,
    cuisinetype,
    pricing_for_2,
    restaurant_phone,
    operatinghours,
    locationid,
    activeflag,
    openstatus,
    locality,
    restaurant_address,
    latitude,
    longitude,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as restaurantid,
        t.$2::text as name,
        t.$3::text as cuisinetype,
        t.$4::text as pricing_for_2,
        t.$5::text as restaurant_phone,
        t.$6::text as operatinghours,
        t.$7::text as locationid,
        t.$8::text as activeflag,
        t.$9::text as openstatus,
        t.$10::text as locality,
        t.$11::text as restaurant_address,
        t.$12::text as latitude,
        t.$13::text as longitude,
        t.$14::text as createddate,
        t.$15::text as modifieddate,
        -- audit columns for tracking & debugging
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @stage_sch.csv_stg/initial/restaurant/restaurant-delhi+NCR.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

/*------------------------------------------------------------------------------
    PART 6: CLEAN LAYER DATA TRANSFORMATION
------------------------------------------------------------------------------*/

-- Transform and load data from stage to clean layer
-- Uses MERGE operation with data type conversion and validation
MERGE INTO clean_sch.restaurant as target
USING (
    SELECT
        try_cast(restaurantid AS number) AS restaurant_id,
        try_cast(name AS string) AS name,
        try_cast(cuisinetype AS string) AS cuisine_type,
        try_cast(pricing_for_2 AS number(10, 2)) AS pricing_for_two,
        try_cast(restaurant_phone AS string) AS restaurant_phone,
        try_cast(operatinghours AS string) AS operating_hours,
        try_cast(locationid AS number) AS location_id_fk,
        try_cast(activeflag AS string) AS active_flag,
        try_cast(openstatus AS string) AS open_status,
        try_cast(locality AS string) AS locality,
        try_cast(restaurant_address AS string) AS restaurant_address,
        try_cast(latitude AS number(9, 6)) AS latitude,
        try_cast(longitude AS number(9, 6)) AS longitude,
        try_to_timestamp_ntz(createddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS created_dt,
        try_to_timestamp_ntz(modifieddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    FROM
        stage_sch.restaurant_stm
) AS SOURCE
ON target.restaurant_id = source.restaurant_id
WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.cuisine_type = source.cuisine_type,
        target.pricing_for_two = source.pricing_for_two,
        target.restaurant_phone = source.restaurant_phone,
        target.operating_hours = source.operating_hours,
        target.location_id_fk = source.location_id_fk,
        target.active_flag = source.active_flag,
        target.open_status = source.open_status,
        target.locality = source.locality,
        target.restaurant_address = source.restaurant_address,
        target.latitude = source.latitude,
        target.longitude = source.longitude,
        target.created_dt = source.created_dt,
        target.modified_dt = source.modified_dt,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5
WHEN NOT MATCHED THEN
    INSERT (
        restaurant_id,
        name,
        cuisine_type,
        pricing_for_two,
        restaurant_phone,
        operating_hours,
        location_id_fk,
        active_flag,
        open_status,
        locality,
        restaurant_address,
        latitude,
        longitude,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    )
    VALUES (
        source.restaurant_id,
        source.name,
        source.cuisine_type,
        source.pricing_for_two,
        source.restaurant_phone,
        source.operating_hours,
        source.location_id_fk,
        source.active_flag,
        source.open_status,
        source.locality,
        source.restaurant_address,
        source.latitude,
        source.longitude,
        source.created_dt,
        source.modified_dt,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5
    );

/*------------------------------------------------------------------------------
    PART 7: CONSUMPTION LAYER DATA TRANSFORMATION (SCD TYPE 2)
------------------------------------------------------------------------------*/

-- Load data from clean to consumption layer with SCD Type 2 implementation
-- Handles current and historical record versioning
MERGE INTO
    CONSUMPTION_SCH.RESTAURANT_DIM AS target
USING
    CLEAN_SCH.RESTAURANT_STM AS source
ON
    target.RESTAURANT_ID = source.RESTAURANT_ID AND
    target.NAME = source.NAME AND
    target.CUISINE_TYPE = source.CUISINE_TYPE AND
    target.PRICING_FOR_TWO = source.PRICING_FOR_TWO AND
    target.RESTAURANT_PHONE = source.RESTAURANT_PHONE AND
    target.OPERATING_HOURS = source.OPERATING_HOURS AND
    target.LOCATION_ID_FK = source.LOCATION_ID_FK AND
    target.ACTIVE_FLAG = source.ACTIVE_FLAG AND
    target.OPEN_STATUS = source.OPEN_STATUS AND
    target.LOCALITY = source.LOCALITY AND
    target.RESTAURANT_ADDRESS = source.RESTAURANT_ADDRESS AND
    target.LATITUDE = source.LATITUDE AND
    target.LONGITUDE = source.LONGITUDE
WHEN MATCHED
    AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET
        target.EFF_END_DATE = CURRENT_TIMESTAMP(),
        target.IS_CURRENT = FALSE
WHEN NOT MATCHED
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    INSERT (
        RESTAURANT_HK,
        RESTAURANT_ID,
        NAME,
        CUISINE_TYPE,
        PRICING_FOR_TWO,
        RESTAURANT_PHONE,
        OPERATING_HOURS,
        LOCATION_ID_FK,
        ACTIVE_FLAG,
        OPEN_STATUS,
        LOCALITY,
        RESTAURANT_ADDRESS,
        LATITUDE,
        LONGITUDE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(
            SHA1_hex(
                CONCAT(
                    source.RESTAURANT_ID,
                    source.NAME,
                    source.CUISINE_TYPE,
                    source.PRICING_FOR_TWO,
                    source.RESTAURANT_PHONE,
                    source.OPERATING_HOURS,
                    source.LOCATION_ID_FK,
                    source.ACTIVE_FLAG,
                    source.OPEN_STATUS,
                    source.LOCALITY,
                    source.RESTAURANT_ADDRESS,
                    source.LATITUDE, source.LONGITUDE
                )
            )
        ),
        source.RESTAURANT_ID,
        source.NAME,
        source.CUISINE_TYPE,
        source.PRICING_FOR_TWO,
        source.RESTAURANT_PHONE,
        source.OPERATING_HOURS,
        source.LOCATION_ID_FK,
        source.ACTIVE_FLAG,
        source.OPEN_STATUS,
        source.LOCALITY,
        source.RESTAURANT_ADDRESS,
        source.LATITUDE,
        source.LONGITUDE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
WHEN NOT MATCHED
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    INSERT (
        RESTAURANT_HK,
        RESTAURANT_ID,
        NAME,
        CUISINE_TYPE,
        PRICING_FOR_TWO,
        RESTAURANT_PHONE,
        OPERATING_HOURS,
        LOCATION_ID_FK,
        ACTIVE_FLAG,
        OPEN_STATUS,
        LOCALITY,
        RESTAURANT_ADDRESS,
        LATITUDE,
        LONGITUDE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(
            SHA1_hex(
                CONCAT(
                    source.RESTAURANT_ID,
                    source.NAME,
                    source.CUISINE_TYPE,
                    source.PRICING_FOR_TWO,
                    source.RESTAURANT_PHONE,
                    source.OPERATING_HOURS,
                    source.LOCATION_ID_FK,
                    source.ACTIVE_FLAG,
                    source.OPEN_STATUS,
                    source.LOCALITY,
                    source.RESTAURANT_ADDRESS,
                    source.LATITUDE,
                    source.LONGITUDE
                )
            )
        ),
        source.RESTAURANT_ID,
        source.NAME,
        source.CUISINE_TYPE,
        source.PRICING_FOR_TWO,
        source.RESTAURANT_PHONE,
        source.OPERATING_HOURS,
        source.LOCATION_ID_FK,
        source.ACTIVE_FLAG,
        source.OPEN_STATUS,
        source.LOCALITY,
        source.RESTAURANT_ADDRESS,
        source.LATITUDE,
        source.LONGITUDE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );

/*------------------------------------------------------------------------------
    PART 8: DELTA DATA LOADING - NEW RECORDS
------------------------------------------------------------------------------*/

-- List delta files to be processed
list @stage_sch.csv_stg/delta/restaurant;

-- Load delta data (new records) into stage layer
copy into stage_sch.restaurant (
    restaurantid,
    name,
    cuisinetype,
    pricing_for_2,
    restaurant_phone,
    operatinghours,
    locationid,
    activeflag,
    openstatus,
    locality,
    restaurant_address,
    latitude,
    longitude,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as restaurantid,        -- restaurantid as the first column
        t.$2::text as name,
        t.$3::text as cuisinetype,
        t.$4::text as pricing_for_2,
        t.$5::text as restaurant_phone,
        t.$6::text as operatinghours,
        t.$7::text as locationid,
        t.$8::text as activeflag,
        t.$9::text as openstatus,
        t.$10::text as locality,
        t.$11::text as restaurant_address,
        t.$12::text as latitude,
        t.$13::text as longitude,
        t.$14::text as createddate,
        t.$15::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @stage_sch.csv_stg/delta/restaurant/day-01-insert-restaurant-delhi+NCR.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

/*------------------------------------------------------------------------------
    PART 9: DELTA DATA LOADING - UPDATES
------------------------------------------------------------------------------*/

-- List delta files for updates
list @stage_sch.csv_stg/delta/restaurant;

-- Load delta updates into stage layer
copy into stage_sch.restaurant (
    restaurantid,
    name,
    cuisinetype,
    pricing_for_2,
    restaurant_phone,
    operatinghours,
    locationid,
    activeflag,
    openstatus,
    locality,
    restaurant_address,
    latitude,
    longitude,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as restaurantid,        -- restaurantid as the first column
        t.$2::text as name,
        t.$3::text as cuisinetype,
        t.$4::text as pricing_for_2,
        t.$5::text as restaurant_phone,
        t.$6::text as operatinghours,
        t.$7::text as locationid,
        t.$8::text as activeflag,
        t.$9::text as openstatus,
        t.$10::text as locality,
        t.$11::text as restaurant_address,
        t.$12::text as latitude,
        t.$13::text as longitude,
        t.$14::text as createddate,
        t.$15::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @stage_sch.csv_stg/delta/restaurant/day-02-upsert-restaurant-delhi+NCR.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

/*------------------------------------------------------------------------------
    CLEAN UP STATEMENTS
    These statements are used to clean up existing objects for re-execution
------------------------------------------------------------------------------*/

-- Drop stage schema objects (commented out for safety)
-- drop table stage_sch.restaurant;
-- drop stream stage_sch.restaurant_stm;

/*------------------------------------------------------------------------------
    DIAGNOSTIC QUERIES
    These queries are used throughout the script for validation and debugging
------------------------------------------------------------------------------*/

-- List files in stage
-- list @stage_sch.csv_stg/initial/restaurant;

-- Stage layer validation queries
-- select * from stage_sch.restaurant;
-- select * from stage_sch.restaurant_stm;

-- Clean layer validation queries
-- select * from clean_sch.restaurant;
-- select RESTAURANT_ID,METADATA$ISUPDATE,* from clean_sch.restaurant_stm;

-- Consumption layer validation queries
-- select * from consumption_sch.restaurant_dim;