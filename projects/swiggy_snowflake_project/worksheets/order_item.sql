/*------------------------------------------------------------------------------
    ORDER ITEM DATA WAREHOUSE ETL WORKFLOW

    This script implements a complete ETL process for order item data in Snowflake,
    following a multi-layer data warehouse architecture:

    - Stage Layer: Raw data landing from external sources
    - Clean Layer: Data with proper types and business rules applied

    The script handles initial load and delta processing with appropriate auditing.
------------------------------------------------------------------------------*/

/*------------------------------------------------------------------------------
                                    System Steps
------------------------------------------------------------------------------*/
-- Create a table for order item entity in stage layer
-- Create a stream object for order item entity in stage layer
-- Create a table for order item entity in clean layer
-- Stage Layer
    -- create table
    -- create stream object
-- Clean Layer
    -- create table
    -- create stream object
-- Write Merge Statements
    -- Statement to merge the data from Stage to Clean
-- Write Copy Statement
    -- Statement to copy file from Stages partitions to Stage table
-- Initial Load
    -- list the files in initial location
    -- Run copy command for loading data from Stages to stage table
-- If initial load is successful then perform delta load
-- Delta Load
    -- Run merge statement from stage to clean

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

-- Create Order Item table in stage schema with text data types
-- This ensures data loads without conversion errors and preserves original format
create or replace table stage_sch.order_item (
    orderitemid text comment 'Primary Key (Source System)',         -- primary key as text
    orderid text comment 'Order FK(Source System)',                 -- foreign key reference as text (no constraint in snowflake)
    menuid text comment 'Menu FK(Source System)',                   -- foreign key reference as text (no constraint in snowflake)
    quantity text,                                                  -- quantity as text
    price text,                                                     -- price as text (no decimal constraint)
    subtotal text,                                                  -- subtotal as text (no decimal constraint)
    createddate text,                                               -- created date as text
    modifieddate text,                                              -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the order item stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

-- Create append-only stream on order item table for change data capture
-- This enables tracking of all new data loaded into the stage table
create or replace stream stage_sch.order_item_stm
on table stage_sch.order_item
append_only = true
comment = 'This is the append-only stream object on order item table that only gets delta data';

/*------------------------------------------------------------------------------
    PART 3: CLEAN LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Create Order Item table in clean schema with proper data types
-- This represents the single source of truth with validation and standardization
create or replace table clean_sch.order_item (
    order_item_sk NUMBER AUTOINCREMENT primary key comment 'Surrogate Key (EDW)',   -- Auto-incremented unique identifier for each order item
    order_item_id NUMBER  NOT NULL UNIQUE comment 'Primary Key (Source System)',
    order_id_fk NUMBER  NOT NULL comment 'Order FK(Source System)',                 -- Foreign key reference for Order ID
    menu_id_fk NUMBER  NOT NULL comment 'Menu FK(Source System)',                   -- Foreign key reference for Menu ID
    quantity NUMBER(10, 2),                                                         -- Quantity as a decimal number
    price NUMBER(10, 2),                                                            -- Price as a decimal number
    subtotal NUMBER(10, 2),                                                         -- Subtotal as a decimal number
    created_dt TIMESTAMP,                                                           -- Created date of the order item
    modified_dt TIMESTAMP,                                                          -- Modified date of the order item

    -- Audit columns
    _stg_file_name VARCHAR(255),                                                    -- File name of the staging file
    _stg_file_load_ts TIMESTAMP,                                                    -- Timestamp when the file was loaded
    _stg_file_md5 VARCHAR(255),                                                     -- MD5 hash of the file for integrity check
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP                               -- Timestamp when data is copied into the clean layer
)
comment = 'Order item entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- Create standard stream on clean order item table
-- Tracks all data changes for downstream processing
create or replace stream clean_sch.order_item_stm
on table clean_sch.order_item
comment = 'This is the stream object on order_item table table to track insert, update, and delete changes';

/*------------------------------------------------------------------------------
    PART 4: STAGE TO CLEAN LAYER TRANSFORMATION
------------------------------------------------------------------------------*/

-- Transform and load data from stage to clean layer
-- Applies data type conversion and validation during the load process
merge into clean_sch.order_item as target
using
    stage_sch.order_item_stm as source
on
    target.order_item_id = source.orderitemid and
    target.order_id_fk = source.orderid and
    target.menu_id_fk = source.menuid
when matched then
    update set
        target.quantity = source.quantity,
        target.price = source.price,
        target.subtotal = source.subtotal,
        target.created_dt = source.createddate,
        target.modified_dt = source.modifieddate,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
when not matched then
    insert (
        order_item_id,
        order_id_fk,
        menu_id_fk,
        quantity,
        price,
        subtotal,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    values (
        source.orderitemid,
        source.orderid,
        source.menuid,
        source.quantity,
        source.price,
        source.subtotal,
        source.createddate,
        source.modifieddate,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        CURRENT_TIMESTAMP()
    );

/*------------------------------------------------------------------------------
    PART 5: DELTA DATA LOADING
------------------------------------------------------------------------------*/

-- List files in delta directory to check for new data
list @stage_sch.csv_stg/delta/day-01-order-item.csv;

-- Load delta order item data
-- Processes new and modified records from the latest data file
copy into stage_sch.order_item (
    orderitemid,
    orderid,
    menuid,
    quantity,
    price,
    subtotal,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as orderitemid,
        t.$2::text as orderid,
        t.$3::text as menuid,
        t.$4::text as quantity,
        t.$5::text as price,
        t.$6::text as subtotal,
        t.$7::text as createddate,
        t.$8::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/delta/order-item/day-02-order-item.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

/*------------------------------------------------------------------------------
    CLEAN UP STATEMENTS
    These statements are used to clean up existing objects for re-execution
------------------------------------------------------------------------------*/

-- Drop stage schema objects (commented out for safety)
-- DROP TABLE stage_sch.order_item;
-- DROP STREAM stage_sch.order_item_stm;
-- DROP TABLE clean_sch.order_item;
-- DROP STREAM clean_sch.order_item_stm;

/*------------------------------------------------------------------------------
    DIAGNOSTIC QUERIES
    These queries are used throughout the script for validation and debugging
------------------------------------------------------------------------------*/

-- Stage layer validation queries
-- select * from stage_sch.order_item;
-- select * from stage_sch.order_item_stm;

-- Clean layer validation queries
-- select * from clean_sch.order_item;
-- select * from clean_sch.order_item_stm;