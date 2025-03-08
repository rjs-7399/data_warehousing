/*------------------------------------------------------------------------------
    RESTAURANT LOCATION DATA WAREHOUSE ETL WORKFLOW

    This script implements a complete ETL process for location data in Snowflake,
    following a multi-layer data warehouse architecture:

    - Stage Layer: Raw data landing from external sources
    - Clean Layer: Data with proper types and business rules applied
    - Consumption Layer: Dimensional model with SCD Type 2 implementation

    The script handles initial load and delta processing with error handling.
------------------------------------------------------------------------------*/

/*------------------------------------------------------------------------------
    PART 1: INITIAL SETUP AND DATA LOADING
------------------------------------------------------------------------------*/

-- Set up the session context
use role sysadmin;
use warehouse adhoc_wh;
use database sandbox;
use schema stage_sch;

-- Query sample data from external stage to inspect structure
-- This helps validate the file format and data types before loading
select
    t.$1::text as locationid,
    t.$2::text as city,
    t.$3::text as state,
    t.$4::text as zipcode,
    t.$5::text as activeflag,
    t.$6::text as createdate,
    t.$7::text as modifieddate
from @stage_sch.csv_stg/initial/location/location-5rows.csv
(file_format => 'stage_sch.csv_file_format') t;

-- Query data again with additional audit columns for tracking
-- These columns help with data lineage and troubleshooting
select
    t.$1::text as locationid,
    t.$2::text as city,
    t.$3::text as state,
    t.$4::text as zipcode,
    t.$5::text as activeflag,
    t.$6::text as createdate,
    t.$7::text as modifieddate,

    --- Audit columns for tracking & debugging
    metadata$filename as _stg_file_name,
    metadata$file_last_modified as _stg_file_load_ts,
    metadata$file_content_key as _stg_file_md5,
    current_timestamp as _copy_data_ts
from @stage_sch.csv_stg/initial/location/location-5rows.csv
(file_format => 'stage_sch.csv_file_format') t;


-- Create stage table for raw location data
-- All columns are text to avoid load errors; transformations happen in later layers
create table stage_sch.location (
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createddate text,
    modifieddate text,
    --- audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the lication stage/raw table where data will be copied from internal stage using copy command. This is as-is data representation from the source location. All the columns are text datatype except the audit columns that are added for traceability.';


-- Create append-only stream on location table to capture changes
-- This enables CDC (Change Data Capture) for downstream processing
create or replace stream stage_sch.location_stm
on table stage_sch.location
append_only = true
comment = 'this is the append-only stream object on location table that gets delta data based on changes';


-- Load initial data into stage table using COPY command
-- Applies light transformation and adds metadata during load
copy into stage_sch.location (
    locationid,
    city,
    state,
    zipcode,
    activeflag,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/initial/location t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;


/*------------------------------------------------------------------------------
    PART 2: CLEAN LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Switch to clean schema for the next transformation step
use schema clean_sch;

-- Create clean layer table with proper data types and business rules
-- This represents the "single source of truth" with validated data
create or replace table clean_sch.restaurant_location (
    restaurant_location_sk number autoincrement primary key,
    location_id number not null unique,
    city string(100) not null,
    state string(100) not null,
    state_code string(2) not null,
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier text(6),
    zip_code string(10) not null,
    active_flag string(10) not null,
    created_ts timestamp_tz not null,
    modified_ts timestamp_tz,

    -- additional audit columns
    _stg_file_name string,
    _stg_file_load_ts timestamp_tz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
)
comment = 'Location entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';


-- Create standard stream for tracking all DML operations
create or replace stream clean_sch.restaurant_location_stm
on table clean_sch.restaurant_location
comment = 'This is a standard stream object on the location table to track insert, update and delete changes';

-- MERGE statement to load and transform data from stage to clean layer
-- Includes data enrichment, standardization, and business rule application
MERGE INTO clean_sch.restaurant_location AS target
USING (
    SELECT
        CAST(LocationID AS NUMBER) AS Location_ID,
        CAST(City AS STRING) AS City,
        CASE
            WHEN CAST(State AS STRING) = 'Delhi' THEN 'New Delhi'
            ELSE CAST(State AS STRING)
        END AS State,
        -- State Code Mapping
        CASE
            WHEN State = 'Delhi' THEN 'DL'
            WHEN State = 'Maharashtra' THEN 'MH'
            WHEN State = 'Uttar Pradesh' THEN 'UP'
            WHEN State = 'Gujarat' THEN 'GJ'
            WHEN State = 'Rajasthan' THEN 'RJ'
            WHEN State = 'Kerala' THEN 'KL'
            WHEN State = 'Punjab' THEN 'PB'
            WHEN State = 'Karnataka' THEN 'KA'
            WHEN State = 'Madhya Pradesh' THEN 'MP'
            WHEN State = 'Odisha' THEN 'OR'
            WHEN State = 'Chandigarh' THEN 'CH'
            WHEN State = 'West Bengal' THEN 'WB'
            WHEN State = 'Sikkim' THEN 'SK'
            WHEN State = 'Andhra Pradesh' THEN 'AP'
            WHEN State = 'Assam' THEN 'AS'
            WHEN State = 'Jammu and Kashmir' THEN 'JK'
            WHEN State = 'Puducherry' THEN 'PY'
            WHEN State = 'Uttarakhand' THEN 'UK'
            WHEN State = 'Himachal Pradesh' THEN 'HP'
            WHEN State = 'Tamil Nadu' THEN 'TN'
            WHEN State = 'Goa' THEN 'GA'
            WHEN State = 'Telangana' THEN 'TG'
            WHEN State = 'Chhattisgarh' THEN 'CG'
            WHEN State = 'Jharkhand' THEN 'JH'
            WHEN State = 'Bihar' THEN 'BR'
            ELSE NULL
        END AS state_code,
        CASE
            WHEN State IN ('Delhi', 'Chandigarh', 'Puducherry', 'Jammu and Kashmir') THEN 'Y'
            ELSE 'N'
        END AS is_union_territory,
        CASE
            WHEN (State = 'Delhi' AND City = 'New Delhi') THEN TRUE
            WHEN (State = 'Maharashtra' AND City = 'Mumbai') THEN TRUE
            -- Other conditions for capital cities
            ELSE FALSE
        END AS capital_city_flag,
        CASE
            WHEN City IN (
                'Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad'
            ) THEN 'Tier-1'
            WHEN City IN (
                'Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara', 'Coimbatore',
                'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati', 'Chandigarh'
            ) THEN 'Tier-2'
            ELSE 'Tier-3'
        END AS city_tier,
        CAST(ZipCode AS STRING) AS Zip_Code,
        CAST(ActiveFlag AS STRING) AS Active_Flag,
        TO_TIMESTAMP_TZ(CreatedDate, 'YYYY-MM-DD HH24:MI:SS') AS created_ts,
        TO_TIMESTAMP_TZ(ModifiedDate, 'YYYY-MM-DD HH24:MI:SS') AS modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts
    FROM stage_sch.location_stm
) AS source
ON target.Location_ID = source.Location_ID
WHEN MATCHED AND (
    target.City != source.City OR
    target.State != source.State OR
    target.state_code != source.state_code OR
    target.is_union_territory != source.is_union_territory OR
    target.capital_city_flag != source.capital_city_flag OR
    target.city_tier != source.city_tier OR
    target.Zip_Code != source.Zip_Code OR
    target.Active_Flag != source.Active_Flag OR
    target.modified_ts != source.modified_ts
) THEN
    UPDATE SET
        target.City = source.City,
        target.State = source.State,
        target.state_code = source.state_code,
        target.is_union_territory = source.is_union_territory,
        target.capital_city_flag = source.capital_city_flag,
        target.city_tier = source.city_tier,
        target.Zip_Code = source.Zip_Code,
        target.Active_Flag = source.Active_Flag,
        target.modified_ts = source.modified_ts,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    INSERT (
        Location_ID,
        City,
        State,
        state_code,
        is_union_territory,
        capital_city_flag,
        city_tier,
        Zip_Code,
        Active_Flag,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        source.Location_ID,
        source.City,
        source.State,
        source.state_code,
        source.is_union_territory,
        source.capital_city_flag,
        source.city_tier,
        source.Zip_Code,
        source.Active_Flag,
        source.created_ts,
        source.modified_ts,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

/*------------------------------------------------------------------------------
    PART 3: CONSUMPTION LAYER IMPLEMENTATION (SCD TYPE 2)
------------------------------------------------------------------------------*/

-- Create dimensional table in consumption layer for analytics
-- Implements Slowly Changing Dimension Type 2 for historical tracking
create or replace table consumption_sch.restaurant_location_dim (
    restaurant_location_hk NUMBER primary key,          -- hash key for the dimension
    location_id number(38,0) not null,                  -- business key
    city varchar(100) not null,                         -- city
    state varchar(100) not null,                        -- state
    state_code varchar(2) not null,                     -- state code
    is_union_territory boolean not null default false,  -- union territory flag
    capital_city_flag boolean not null default false,   -- capital city flag
    city_tier varchar(6),                               -- city tier
    zip_code varchar(10) not null,                      -- zip code
    active_flag varchar(10) not null,                   -- active flag (indicating current record)
    eff_start_dt timestamp_tz(9) not null,              -- effective start date for scd2
    eff_end_dt timestamp_tz(9),                         -- effective end date for scd2
    current_flag boolean not null default true          -- indicator of the current record
)
comment = 'Dimension table for restaurant location with scd2 (slowly changing dimension) enabled and hashkey as surrogate key';

-- MERGE statement to implement SCD Type 2 logic
-- Handles inserts, updates with versioning, and logical deletes
MERGE INTO
        CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM AS target
    USING
        CLEAN_SCH.RESTAURANT_LOCATION_STM AS source
    ON
        target.LOCATION_ID = source.LOCATION_ID and
        target.ACTIVE_FLAG = source.ACTIVE_FLAG
    WHEN MATCHED
        AND source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET
        target.EFF_END_DT = CURRENT_TIMESTAMP(),
        target.CURRENT_FLAG = FALSE
    WHEN NOT MATCHED
        AND source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE'
    THEN
    -- Insert new record with current data and new effective start date
    INSERT  (
        RESTAURANT_LOCATION_HK,
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        ACTIVE_FLAG,
        EFF_START_DT,
        EFF_END_DT,
        CURRENT_FLAG
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
        source.LOCATION_ID,
        source.CITY,
        source.STATE,
        source.STATE_CODE,
        source.IS_UNION_TERRITORY,
        source.CAPITAL_CITY_FLAG,
        source.CITY_TIER,
        source.ZIP_CODE,
        source.ACTIVE_FLAG,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
    WHEN NOT MATCHED AND
    source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        RESTAURANT_LOCATION_HK,
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        ACTIVE_FLAG,
        EFF_START_DT,
        EFF_END_DT,
        CURRENT_FLAG
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
        source.LOCATION_ID,
        source.CITY,
        source.STATE,
        source.STATE_CODE,
        source.IS_UNION_TERRITORY,
        source.CAPITAL_CITY_FLAG,
        source.CITY_TIER,
        source.ZIP_CODE,
        source.ACTIVE_FLAG,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );


/*------------------------------------------------------------------------------
    PART 4: DELTA DATA LOADING AND PROCESSING
------------------------------------------------------------------------------*/

-- List files in delta directory to check for new data
list @stage_sch.csv_stg/delta/location/;

-- Load delta data batch 1 (new records) into stage layer
copy into stage_sch.location (
    locationid,
    city,
    state,
    zipcode,
    activeflag,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/delta/location/delta-day01-2rows-add.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

-- Check for files with delta updates (day 2)
list @stage_sch.csv_stg/delta/location/;

-- Load delta data batch 2 (updates to existing records) into stage layer
copy into stage_sch.location (
    locationid,
    city,
    state,
    zipcode,
    activeflag,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/delta/location/delta-day02-2rows-update.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;


/*------------------------------------------------------------------------------
    PART 5: ERROR HANDLING - INVALID DELIMITER
------------------------------------------------------------------------------*/

-- Check for files with potential errors
list @stage_sch.csv_stg/delta/location/;

-- Attempt to load data with invalid delimiter
-- This will likely fail due to delimiter mismatch with the file format
copy into stage_sch.location (
    locationid,
    city,
    state,
    zipcode,
    activeflag,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/delta/location/delta-day03-invalid-delimiter.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

-- Create temporary table to store problematic records for analysis
create temp table common.location_tmp as
select * from stage_sch.location_stm;

-- Clean up bad data from stage table
-- Identifying records with pipe character which indicates delimiter issues
delete from stage_sch.location where locationid like '%|%';


/*------------------------------------------------------------------------------
    PART 6: ERROR HANDLING - JUNK DATA
------------------------------------------------------------------------------*/

-- Check for files with potential junk data
list @stage_sch.csv_stg/delta/location/;

-- Attempt to load data that may contain junk records
copy into stage_sch.location (
    locationid,
    city,
    state,
    zipcode,
    activeflag,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/delta/location/delta-day04-invalid-files.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

-- Save problematic records to a temporary table for analysis
create table common.location_junk_tmp as
select * from stage_sch.location_stm;


-- Clean up junk data from stage table
delete from stage_sch.location where locationid like '%junk%';


-- Clean up existing objects if needed for re-execution
DROP TABLE IF EXISTS stage_sch.location;
DROP STREAM IF EXISTS stage_sch.location_stm;


-- Clean up existing objects for re-execution
DROP TABLE IF EXISTS clean_sch.restaurant_location;
DROP STREAM IF EXISTS stage_sch.restaurant_location_stm;
/*------------------------------------------------------------------------------
    DIAGNOSTIC QUERIES
    These queries are used throughout the script for validation and debugging
------------------------------------------------------------------------------*/

-- Stage layer validation queries
-- select * from stage_sch.location;
-- select * from stage_sch.location_stm;

-- Clean layer validation queries
-- select * from clean_sch.restaurant_location;
-- select * from clean_sch.restaurant_location_stm;
-- select LOCATION_ID, CITY, ACTIVE_FLAG, METADATA$ACTION, METADATA$ISUPDATE from clean_sch.restaurant_location_stm;

-- Consumption layer validation queries
-- select * from consumption_sch.restaurant_location_dim;

-- Error handling validation queries
-- select LOCATIONID,_STG_FILE_NAME,_COPY_DATA_TS,* from stage_sch.location_stm;
-- select * from common.location_tmp;
-- select * from common.location_junk_tmp;