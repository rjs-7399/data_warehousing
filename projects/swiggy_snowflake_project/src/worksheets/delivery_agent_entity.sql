/*------------------------------------------------------------------------------
    DELIVERY AGENT DATA WAREHOUSE ETL WORKFLOW

    This script implements a complete ETL process for delivery agent data in Snowflake,
    following a multi-layer data warehouse architecture:

    - Stage Layer: Raw data landing from external sources
    - Clean Layer: Data with proper types and business rules applied
    - Consumption Layer: Dimensional model with SCD Type 2 implementation

    The script handles initial load and delta processing with appropriate auditing.
------------------------------------------------------------------------------*/

/*------------------------------------------------------------------------------
                                    System Steps
------------------------------------------------------------------------------*/
-- Create a table for delivery agent entity in stage layer
-- Create a stream object for delivery agent entity in stage layer
-- Create a table for delivery agent entity in clean layer
-- Stage Layer
    -- create table
    -- create stream object
-- Clean Layer
    -- create table
    -- create stream object
-- Consumption Layer
    -- create table
-- Write Merge Statements
    -- Statement to merge the data from Stage to Clean
    -- Statement to merge the data from Clean to Consumption
-- Write Copy Statement
    -- Statement to copy file from Stages partitions to Stage table.
-- Initial Load
    -- list the files in initial location
    -- Run copy command for loading data from Stages to stage table.
-- If initial load is successful then perform delta load
-- Delta Load
    -- Run merge statement from stage to clean.
    -- Run merge statement from clean to consumption.

/*------------------------------------------------------------------------------
    PART 1: INITIAL SETUP AND CONTEXT
------------------------------------------------------------------------------*/

-- Set up the session context
use role sysadmin;
use warehouse adhoc_wh;
use database sandbox;
use schema stage_sch;

/*------------------------------------------------------------------------------
    PART 2: STAGE LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Create Delivery Agent table in stage schema with text data types
-- This ensures data loads without conversion errors and preserves original format
create or replace table a.deliveryagent (
    deliveryagentid text comment 'Primary Key (Source System)',         -- primary key as text
    name text,                                                          -- name as text, required field
    phone text,                                                         -- phone as text, unique constraint indicated
    vehicletype text,                                                   -- vehicle type as text
    locationid text,                                                    -- foreign key reference as text (no constraint in snowflake)
    status text,                                                        -- status as text
    gender text,                                                        -- status as text
    rating text,                                                        -- rating as text
    createddate text,                                                   -- created date as text
    modifieddate text,                                                  -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the delivery stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

-- Create append-only stream on delivery agent table for change data capture
-- This enables tracking of all new data loaded into the stage table
create or replace stream stage_sch.deliveryagent_stm
on table stage_sch.deliveryagent
append_only = true
comment = 'This is the append-only stream object on delivery agent table that only gets delta data';

/*------------------------------------------------------------------------------
    PART 3: CLEAN LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Create Delivery Agent table in clean schema with proper data types
-- This represents the single source of truth with validation and standardization
create or replace table clean_sch.deliveryagent (
    delivery_agent_sk INT AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)',      -- Primary key with auto-increment
    delivery_agent_id INT NOT NULL UNIQUE comment 'Primary Key (Source System)',        -- Delivery agent ID as integer
    name STRING NOT NULL,                                                               -- Name as string, required field
    phone STRING NOT NULL,                                                              -- Phone as string, unique constraint
    vehicle_type STRING NOT NULL,                                                       -- Vehicle type as string
    location_id_fk INT comment 'Location FK(Source System)',                            -- Location ID as integer
    status STRING,                                                                      -- Status as string
    gender STRING,                                                                      -- Gender as string
    rating number(4,2),                                                                 -- Rating as float
    created_dt TIMESTAMP_NTZ,                                                           -- Created date as timestamp without timezone
    modified_dt TIMESTAMP_NTZ,                                                          -- Modified date as timestamp without timezone

    -- Audit columns with appropriate data types
    _stg_file_name STRING,                                                              -- Staging file name as string
    _stg_file_load_ts TIMESTAMP,                                                        -- Staging file load timestamp
    _stg_file_md5 STRING,                                                               -- Staging file MD5 hash as string
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP                                   -- Data copy timestamp with default value
)
comment = 'Delivery entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- Create stream on clean delivery agent table to track changes
-- This enables comprehensive change tracking for dimensional modeling
create or replace stream clean_sch.deliveryagent_stm
on table clean_sch.deliveryagent
append_only = true
comment = 'This is the stream object on delivery agent table table to track insert, update, and delete changes';

/*------------------------------------------------------------------------------
    PART 4: CONSUMPTION LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Create Delivery Agent dimension table in consumption schema
-- Implements SCD Type 2 pattern for historical tracking
create or replace table consumption_sch.deliveryagent_dim (
    delivery_agent_hk number primary key comment 'Delivery Agend Dim HK (EDW)',             -- Hash key for unique identification
    delivery_agent_id NUMBER not null comment 'Primary Key (Source System)',                -- Business key
    name STRING NOT NULL,                                                                   -- Delivery agent name
    phone STRING UNIQUE,                                                                    -- Phone number, unique
    vehicle_type STRING,                                                                    -- Type of vehicle
    location_id_fk NUMBER NOT NULL comment 'Location FK (Source System)',                   -- Location ID
    status STRING,                                                                          -- Current status of the delivery agent
    gender STRING,                                                                          -- Gender
    rating NUMBER(4,2),                                                                     -- Rating with one decimal precision
    eff_start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,                                     -- Effective start date
    eff_end_date TIMESTAMP,                                                                 -- Effective end date (NULL for active record)
    is_current BOOLEAN DEFAULT TRUE
)
comment = 'Dim table for delivery agent entity with SCD2 support.';

/*------------------------------------------------------------------------------
    PART 5: STAGE TO CLEAN LAYER TRANSFORMATION
------------------------------------------------------------------------------*/

-- Transform and load data from stage to clean layer
-- Applies data type conversion, validation, and business rules during the load process
merge into clean_sch.deliveryagent as target
using stage_sch.deliveryagent_stm as source
on target.delivery_agent_id = source.deliveryagentid
when matched then
    update set
        target.phone = source.phone,
        target.vehicle_type = source.vehicletype,
        target.location_id_fk = TRY_TO_NUMBER(source.locationid),
        target.status = source.status,
        target.gender = source.gender,
        target.rating = TRY_TO_DECIMAL(source.rating,4,2),
        target.created_dt = TRY_TO_TIMESTAMP(source.createddate),
        target.modified_dt = TRY_TO_TIMESTAMP(source.modifieddate),
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
when not matched then
    insert (
        delivery_agent_id,
        name,
        phone,
        vehicle_type,
        location_id_fk,
        status,
        gender,
        rating,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    values (
        TRY_TO_NUMBER(source.deliveryagentid),
        source.name,
        source.phone,
        source.vehicletype,
        TRY_TO_NUMBER(source.locationid),
        source.status,
        source.gender,
        TRY_TO_NUMBER(source.rating),
        TRY_TO_TIMESTAMP(source.createddate),
        TRY_TO_TIMESTAMP(source.modifieddate),
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        CURRENT_TIMESTAMP()
    );

/*------------------------------------------------------------------------------
    PART 6: CLEAN TO CONSUMPTION LAYER TRANSFORMATION (SCD TYPE 2)
------------------------------------------------------------------------------*/

-- Transform and load data from clean to consumption layer
-- Implements SCD Type 2 (historical tracking) with effective dates
merge into consumption_sch.deliveryagent_dim as target
using clean_sch.deliveryagent_stm as source
on
    target.delivery_agent_id = source.delivery_agent_id AND
    target.name = source.name AND
    target.phone = source.phone AND
    target.vehicle_type = source.vehicle_type AND
    target.location_id_fk = source.location_id_fk AND
    target.status = source.status AND
    target.gender = source.gender AND
    target.rating = source.rating
when matched
and source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = 'TRUE' then
    update set
        target.eff_end_date = CURRENT_TIMESTAMP,
        target.is_current = FALSE
when not matched
and source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE' then
    insert (
        delivery_agent_hk,        -- Hash key
        delivery_agent_id,
        name,
        phone,
        vehicle_type,
        location_id_fk,
        status,
        gender,
        rating,
        eff_start_date,
        eff_end_date,
        is_current
    )
    values (
        hash(
            SHA1_HEX(
                CONCAT(
                    source.delivery_agent_id,
                    source.name,
                    source.phone,
                    source.vehicle_type,
                    source.location_id_fk,
                    source.status,
                    source.gender, source.rating
                )
            )
        ), -- Hash key
        delivery_agent_id,
        source.name,
        source.phone,
        source.vehicle_type,
        location_id_fk,
        source.status,
        source.gender,
        source.rating,
        CURRENT_TIMESTAMP,       -- Effective start date
        NULL,                    -- Effective end date (NULL for current record)
        TRUE                    -- IS_CURRENT = TRUE for new record
    )
when not matched
and source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE' then
    insert (
        delivery_agent_hk,        -- Hash key
        delivery_agent_id,
        name,
        phone,
        vehicle_type,
        location_id_fk,
        status,
        gender,
        rating,
        eff_start_date,
        eff_end_date,
        is_current
    )
    values (
        hash(
            SHA1_HEX(
                CONCAT(
                    source.delivery_agent_id,
                    source.name,
                    source.phone,
                    source.vehicle_type,
                    source.location_id_fk,
                    source.status,
                    source.gender,
                    source.rating
                )
            )
        ), -- Hash key
        source.delivery_agent_id,
        source.name,
        source.phone,
        source.vehicle_type,
        source.location_id_fk,
        source.status,
        source.gender,
        source.rating,
        CURRENT_TIMESTAMP,       -- Effective start date
        NULL,                    -- Effective end date (NULL for current record)
        TRUE                     -- IS_CURRENT = TRUE for new record
    );

/*------------------------------------------------------------------------------
    PART 7: DELTA DATA LOADING
------------------------------------------------------------------------------*/

-- List files in delta directory to check for new data
list @stage_sch.csv_stg/delta/delivery-agent;

-- Load delta delivery agent data
copy into stage_sch.deliveryagent (
    deliveryagentid,
    name,
    phone,
    vehicletype,
    locationid,
    status,
    gender,
    rating,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as deliveryagentid,
        t.$2::text as name,
        t.$3::text as phone,
        t.$4::text as vehicletype,
        t.$5::text as locationid,
        t.$6::text as status,
        t.$7::text as gender,
        t.$8::text as rating,
        t.$9::text as createddate,
        t.$10::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/delta/delivery-agent/day-02-delivery-agent.csv t
)
file_format = (format_name = stage_sch.csv_file_format)
on_error = abort_statement;

/*------------------------------------------------------------------------------
    CLEAN UP STATEMENTS
    These statements are used to clean up existing objects for re-execution
------------------------------------------------------------------------------*/

-- Drop stage schema objects (commented out for safety)
-- DROP TABLE stage_sch.deliveryagent;
-- DROP STREAM stage_sch.deliveryagent_stm;

/*------------------------------------------------------------------------------
    DIAGNOSTIC QUERIES
    These queries are used throughout the script for validation and debugging
------------------------------------------------------------------------------*/

-- Stage layer validation queries
-- select * from stage_sch.deliveryagent;
-- select * from stage_sch.deliveryagent_stm;

-- Clean layer validation queries
-- select * from clean_sch.deliveryagent;
-- select * from clean_sch.deliveryagent_stm;

-- Consumption layer validation queries
-- select * from consumption_sch.deliveryagent_dim;