/*------------------------------------------------------------------------------
    MENU DATA WAREHOUSE ETL WORKFLOW

    This script implements a complete ETL process for menu data in Snowflake,
    following a multi-layer data warehouse architecture:

    - Stage Layer: Raw data landing from external sources
    - Clean Layer: Data with proper types and business rules applied
    - Consumption Layer: Dimensional model with SCD Type 2 implementation

    The script handles initial load and delta processing with appropriate auditing.
------------------------------------------------------------------------------*/

/*------------------------------------------------------------------------------
                                    System Steps
------------------------------------------------------------------------------*/
-- Create a table for menu entity in stage layer
-- Create a stream object for menu entity in stage layer
-- Create a table for menu entity in clean layer
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
use database sandbox;
use schema stage_sch;
use warehouse adhoc_wh;

/*------------------------------------------------------------------------------
    PART 2: STAGE LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Create Menu table in stage schema with text data types
-- This ensures data loads without conversion errors and preserves original format
create or replace table stage_sch.menu (
    menuid text comment 'Primary Key (Source System)',                  -- primary key as text
    restaurantid text comment 'Restaurant FK(Source System)',           -- foreign key reference as text (no constraint in snowflake)
    itemname text,                                                      -- item name as text
    description text,                                                   -- description as text
    price text,                                                         -- price as text (no decimal constraint)
    category text,                                                      -- category as text
    availability text,                                                  -- availability as text
    itemtype text,                                                      -- item type as text
    createddate text,                                                   -- created date as text
    modifieddate text,                                                  -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the menu stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

-- Create append-only stream on menu table for change data capture
-- This enables tracking of all new data loaded into the stage table
create or replace stream stage_sch.menu_stm
on table stage_sch.menu
append_only = true
comment = 'This is the append-only stream object on menu entity that only gets delta data.';

/*------------------------------------------------------------------------------
    PART 3: CLEAN LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Create Menu table in clean schema with proper data types
-- This represents the single source of truth with validation and standardization
create or replace table clean_sch.menu (
    Menu_SK INT AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)',    -- Auto-incrementing primary key for internal tracking
    Menu_ID INT NOT NULL UNIQUE comment 'Primary Key (Source System)' ,     -- Unique and non-null Menu_ID
    Restaurant_ID_FK INT comment 'Restaurant FK(Source System)' ,           -- Identifier for the restaurant
    Item_Name STRING not null,                                              -- Name of the menu item
    Description STRING not null,                                            -- Description of the menu item
    Price DECIMAL(10, 2) not null,                                          -- Price as a numeric value with 2 decimal places
    Category STRING,                                                        -- Food category (e.g., North Indian)
    Availability BOOLEAN,                                                   -- Availability status (True/False)
    Item_Type STRING,                                                       -- Dietary classification (e.g., Vegan)
    Created_dt TIMESTAMP_NTZ,                                               -- Date when the record was created
    Modified_dt TIMESTAMP_NTZ,                                              -- Date when the record was last modified

    -- Audit columns for traceability
    _STG_FILE_NAME STRING,                                                  -- Source file name
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,                                        -- Timestamp when data was loaded from the staging layer
    _STG_FILE_MD5 STRING,                                                   -- MD5 hash of the source file
    _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP                   -- Timestamp when data was copied to the clean layer
)
comment = 'Menu entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- Create stream on clean menu table to track changes
-- This enables comprehensive change tracking for dimensional modeling
create or replace stream clean_sch.menu_stm
on table clean_sch.menu
append_only = true
comment = 'This is the stream object on menu table table to track insert, update, and delete changes';

/*------------------------------------------------------------------------------
    PART 4: CONSUMPTION LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Create Menu dimension table in consumption schema
-- Implements SCD Type 2 pattern for historical tracking
create or replace table consumption_sch.menu_dim (
    Menu_Dim_HK NUMBER primary key comment 'Menu Dim HK (EDW)',                     -- Hash key generated for Menu Dim table
    Menu_ID INT NOT NULL comment 'Primary Key (Source System)',                     -- Unique and non-null Menu_ID
    Restaurant_ID_FK INT NOT NULL comment 'Restaurant FK (Source System)',          -- Identifier for the restaurant
    Item_Name STRING,                                                               -- Name of the menu item
    Description STRING,                                                             -- Description of the menu item
    Price DECIMAL(10, 2),                                                           -- Price as a numeric value with 2 decimal places
    Category STRING,                                                                -- Food category (e.g., North Indian)
    Availability BOOLEAN,                                                           -- Availability status (True/False)
    Item_Type STRING,                                                               -- Dietary classification (e.g., Vegan)
    EFF_START_DATE TIMESTAMP_NTZ,                                                   -- Effective start date of the record
    EFF_END_DATE TIMESTAMP_NTZ,                                                     -- Effective end date of the record
    IS_CURRENT BOOLEAN                                                              -- Flag to indicate if the record is current (True/False)
)
comment = 'This table stores the dimension data for the menu items, tracking historical changes using SCD Type 2. Each menu item has an effective start and end date, with a flag indicating if it is the current record or historical. The hash key (Menu_Dim_HK) is generated based on Menu_ID and Restaurant_ID.';

/*------------------------------------------------------------------------------
    PART 5: STAGE TO CLEAN LAYER TRANSFORMATION
------------------------------------------------------------------------------*/

-- Transform and load data from stage to clean layer
-- Applies data type conversion, formatting, and validation during the load process
merge into
    clean_sch.menu as target
using (
    select
        TRY_CAST(menuid AS INT) AS Menu_ID,
        TRY_CAST(restaurantid AS INT) AS Restaurant_ID_FK,
        TRIM(itemname) AS Item_Name,
        TRIM(description) AS Description,
        TRY_CAST(price AS DECIMAL(10, 2)) AS Price,
        TRIM(category) AS Category,
        CASE
            WHEN LOWER(availability) = 'true' THEN TRUE
            WHEN LOWER(availability) = 'false' THEN FALSE
            ELSE NULL
        END AS Availability,
        TRIM(itemtype) AS Item_Type,
        TRY_CAST(createddate AS TIMESTAMP_NTZ) AS Created_dt,  -- Renamed column
        TRY_CAST(modifieddate AS TIMESTAMP_NTZ) AS Modified_dt, -- Renamed column
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    from stage_sch.menu
) as source
on target.Menu_ID = source.Menu_ID
when matched then
    update set
        Restaurant_ID_FK = source.Restaurant_ID_FK,
        Item_Name = source.Item_Name,
        Description = source.Description,
        Price = source.Price,
        Category = source.Category,
        Availability = source.Availability,
        Item_Type = source.Item_Type,
        Created_dt = source.Created_dt,
        Modified_dt = source.Modified_dt,
        _STG_FILE_NAME = source._stg_file_name,
        _STG_FILE_LOAD_TS = source._stg_file_load_ts,
        _STG_FILE_MD5 = source._stg_file_md5,
        _COPY_DATA_TS = CURRENT_TIMESTAMP
when not matched then
    insert (
        Menu_ID,
        Restaurant_ID_FK,
        Item_Name,
        Description,
        Price,
        Category,
        Availability,
        Item_Type,
        Created_dt,
        Modified_dt,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    )
    values (
        source.Menu_ID,
        source.Restaurant_ID_FK,
        source.Item_Name,
        source.Description,
        source.Price,
        source.Category,
        source.Availability,
        source.Item_Type,
        source.Created_dt,
        source.Modified_dt,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        CURRENT_TIMESTAMP
    );

/*------------------------------------------------------------------------------
    PART 6: CLEAN TO CONSUMPTION LAYER TRANSFORMATION (SCD TYPE 2)
------------------------------------------------------------------------------*/

-- Transform and load data from clean to consumption layer
-- Implements SCD Type 2 (historical tracking) with effective dates
merge into
    consumption_sch.menu_dim as target
using
    clean_sch.menu_stm as source
on
    target.Menu_ID = source.Menu_ID AND
    target.Restaurant_ID_FK = source.Restaurant_ID_FK AND
    target.Item_Name = source.Item_Name AND
    target.Description = source.Description AND
    target.Price = source.Price AND
    target.Category = source.Category AND
    target.Availability = source.Availability AND
    target.Item_Type = source.Item_Type
when matched
and source.METADATA$ACTION = 'UPDATE' AND source.METADATA$ISUPDATE = 'TRUE' then
    update set
        target.EFF_END_DATE = CURRENT_TIMESTAMP(),
        target.IS_CURRENT = FALSE
when not matched
and source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE' then
    insert (
        Menu_Dim_HK,               -- Hash key
        Menu_ID,
        Restaurant_ID_FK,
        Item_Name,
        Description,
        Price,
        Category,
        Availability,
        Item_Type,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    values (
        hash(
            SHA1_hex(
                CONCAT(
                    source.Menu_ID,
                    source.Restaurant_ID_FK,
                    source.Item_Name,
                    source.Description,
                    source.Price,
                    source.Category,
                    source.Availability,
                    source.Item_Type
                )
            )
        ),  -- Hash key
        source.Menu_ID,
        source.Restaurant_ID_FK,
        source.Item_Name,
        source.Description,
        source.Price,
        source.Category,
        source.Availability,
        source.Item_Type,
        CURRENT_TIMESTAMP(),       -- Effective start date
        NULL,                      -- Effective end date (NULL for current record)
        TRUE                       -- IS_CURRENT = TRUE for new record
    )
when not matched
and source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE' then
    insert (
        Menu_Dim_HK,               -- Hash key
        Menu_ID,
        Restaurant_ID_FK,
        Item_Name,
        Description,
        Price,
        Category,
        Availability,
        Item_Type,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    values (
        hash(
            SHA1_hex(
                CONCAT(
                    source.Menu_ID,
                    source.Restaurant_ID_FK,
                    source.Item_Name,
                    source.Description,
                    source.Price,
                    source.Category,
                    source.Availability,
                    source.Item_Type
                )
            )
        ),  -- Hash key
        source.Menu_ID,
        source.Restaurant_ID_FK,
        source.Item_Name,
        source.Description,
        source.Price,
        source.Category,
        source.Availability,
        source.Item_Type,
        CURRENT_TIMESTAMP(),       -- Effective start date
        NULL,                      -- Effective end date (NULL for current record)
        TRUE                       -- IS_CURRENT = TRUE for new record
    );

/*------------------------------------------------------------------------------
    PART 7: DELTA DATA LOADING
------------------------------------------------------------------------------*/

-- List files in delta directory to check for new data
list @stage_sch.csv_stg/delta/menu;

-- Load delta menu data
copy into stage_sch.menu (
    menuid,
    restaurantid,
    itemname,
    description,
    price,
    category,
    availability,
    itemtype,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as menuid,
        t.$2::text as restaurantid,
        t.$3::text as itemname,
        t.$4::text as description,
        t.$5::text as price,
        t.$6::text as category,
        t.$7::text as availability,
        t.$8::text as itemtype,
        t.$9::text as createddate,
        t.$10::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/delta/menu/day-02-menu-data.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

/*------------------------------------------------------------------------------
    CLEAN UP STATEMENTS
    These statements are used to clean up existing objects for re-execution
------------------------------------------------------------------------------*/

-- Drop stage schema objects (commented out for safety)
-- DROP TABLE stage_sch.menu;
-- DROP STREAM stage_sch.menu_stm;

/*------------------------------------------------------------------------------
    DIAGNOSTIC QUERIES
    These queries are used throughout the script for validation and debugging
------------------------------------------------------------------------------*/

-- Stage layer validation queries
-- select * from stage_sch.menu;
-- select * from stage_sch.menu_stm;

-- Clean layer validation queries
-- select * from clean_sch.menu;
-- select * from clean_sch.menu_stm;

-- Consumption layer validation queries
-- select * from consumption_sch.menu_dim;