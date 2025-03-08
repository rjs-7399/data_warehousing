/*------------------------------------------------------------------------------
    CUSTOMER DATA WAREHOUSE ETL WORKFLOW

    This script implements a complete ETL process for customer data in Snowflake,
    following a multi-layer data warehouse architecture:

    - Stage Layer: Raw data landing from external sources
    - Clean Layer: Data with proper types and business rules applied
    - Consumption Layer: Dimensional model with SCD Type 2 implementation

    The script handles initial load and delta processing with appropriate auditing.
------------------------------------------------------------------------------*/

/*------------------------------------------------------------------------------
                                    System Steps
------------------------------------------------------------------------------*/
-- Create a table for customer entity in stage layer
-- Create a stream object for customer entity in stage layer
-- Create a table for customer entity in clean layer
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
    -- list the files in intial loaction
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

-- Create Customer table in stage schema with text data types
-- This ensures data loads without conversion errors and preserves original format
create table stage_sch.customer (
    customerid text,                                                -- primary key as text
    name text,                                                      -- name as text
    mobile text WITH TAG (common.pii_policy_tag = 'PII'),           -- mobile number as text
    email text WITH TAG (common.pii_policy_tag = 'EMAIL'),          -- email as text
    loginbyusing text,                                              -- login method as text
    gender text WITH TAG (common.pii_policy_tag = 'PII'),           -- gender as text
    dob text WITH TAG (common.pii_policy_tag = 'PII'),              -- date of birth as text
    anniversary text,                                               -- anniversary as text
    preferences text,                                               -- preferences as text
    createddate text,                                               -- created date as text
    modifieddate text,                                              -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the customer stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

-- Create append-only stream on customer table for change data capture
-- This enables tracking of all new data loaded into the stage table
create stream stage_sch.customer_stm
on table stage_sch.customer
append_only = true
comment = 'This is the append-only stream object on customer table that only gets delta data';

/*------------------------------------------------------------------------------
    PART 3: CLEAN LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Create Customer table in clean schema with proper data types
-- This represents the single source of truth with validation and standardization
create or replace table clean_sch.customer (
    CUSTOMER_SK NUMBER AUTOINCREMENT PRIMARY KEY,                   -- Auto-incremented primary key
    CUSTOMER_ID STRING NOT NULL,                                    -- Customer ID
    NAME STRING(100) NOT NULL,                                      -- Customer name
    MOBILE STRING(15)  WITH TAG (common.pii_policy_tag = 'PII'),    -- Mobile number, accommodating international format
    EMAIL STRING(100) WITH TAG (common.pii_policy_tag = 'EMAIL'),   -- Email
    LOGIN_BY_USING STRING(50),                                      -- Method of login (e.g., Social, Google, etc.)
    GENDER STRING(10)  WITH TAG (common.pii_policy_tag = 'PII'),    -- Gender
    DOB DATE WITH TAG (common.pii_policy_tag = 'PII'),              -- Date of birth in DATE format
    ANNIVERSARY DATE,                                               -- Anniversary in DATE format
    PREFERENCES STRING,                                             -- Customer preferences
    CREATED_DT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,              -- Record creation timestamp
    MODIFIED_DT TIMESTAMP_TZ,                                       -- Record modification timestamp, allows NULL if not modified

    -- Additional audit columns
    _STG_FILE_NAME STRING,                                          -- File name for audit
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,                                -- File load timestamp
    _STG_FILE_MD5 STRING,                                           -- MD5 hash for file content
    _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP           -- Copy data timestamp
)
comment = 'Customer entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- Create standard stream on clean customer table
-- Tracks all DML operations (insert, update, delete) for dimensional processing
create stream clean_sch.customer_stm
on table clean_sch.customer
comment = 'This is the stream object on customer entity to track insert, update, and delete changes';

/*------------------------------------------------------------------------------
    PART 4: CONSUMPTION LAYER IMPLEMENTATION
------------------------------------------------------------------------------*/

-- Create Customer dimension table in consumption schema
-- Implements slowly changing dimension type 2 (SCD2) for historical tracking
create table consumption_sch.customer_dim (
    CUSTOMER_HK NUMBER PRIMARY KEY,                                 -- Surrogate key for the customer
    CUSTOMER_ID STRING NOT NULL,                                    -- Natural key for the customer
    NAME STRING(100) NOT NULL,                                      -- Customer name
    MOBILE STRING(15) WITH TAG (common.pii_policy_tag = 'PII'),     -- Mobile number
    EMAIL STRING(100) WITH TAG (common.pii_policy_tag = 'EMAIL'),   -- Email
    LOGIN_BY_USING STRING(50),                                      -- Method of login
    GENDER STRING(10) WITH TAG (common.pii_policy_tag = 'PII'),     -- Gender
    DOB DATE WITH TAG (common.pii_policy_tag = 'PII'),              -- Date of birth
    ANNIVERSARY DATE,                                               -- Anniversary
    PREFERENCES STRING,                                             -- Preferences
    EFF_START_DATE TIMESTAMP_TZ,                                    -- Effective start date
    EFF_END_DATE TIMESTAMP_TZ,                                      -- Effective end date (NULL if active)
    IS_CURRENT BOOLEAN                                              -- Flag to indicate the current record
)
comment = 'Customer Dimension table with SCD Type 2 handling for historical tracking.';

/*------------------------------------------------------------------------------
    PART 5: STAGE TO CLEAN LAYER TRANSFORMATION
------------------------------------------------------------------------------*/

-- Transform and load data from stage to clean layer
-- Applies data type conversion and validation during the load process
MERGE INTO CLEAN_SCH.CUSTOMER AS target
USING (
    SELECT
        CUSTOMERID::STRING AS CUSTOMER_ID,
        NAME::STRING AS NAME,
        MOBILE::STRING AS MOBILE,
        EMAIL::STRING AS EMAIL,
        LOGINBYUSING::STRING AS LOGIN_BY_USING,
        GENDER::STRING AS GENDER,
        TRY_TO_DATE(DOB, 'YYYY-MM-DD') AS DOB,
        TRY_TO_DATE(ANNIVERSARY, 'YYYY-MM-DD') AS ANNIVERSARY,
        PREFERENCES::STRING AS PREFERENCES,
        TRY_TO_TIMESTAMP_TZ(CREATEDDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF6') AS CREATED_DT,
        TRY_TO_TIMESTAMP_TZ(MODIFIEDDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF6') AS MODIFIED_DT,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    FROM STAGE_SCH.CUSTOMER_STM
)    AS source
ON target.CUSTOMER_ID = source.CUSTOMER_ID
WHEN MATCHED THEN
    UPDATE SET
        target.NAME = source.NAME,
        target.MOBILE = source.MOBILE,
        target.EMAIL = source.EMAIL,
        target.LOGIN_BY_USING = source.LOGIN_BY_USING,
        target.GENDER = source.GENDER,
        target.DOB = source.DOB,
        target.ANNIVERSARY = source.ANNIVERSARY,
        target.PREFERENCES = source.PREFERENCES,
        target.CREATED_DT = source.CREATED_DT,
        target.MODIFIED_DT = source.MODIFIED_DT,
        target._STG_FILE_NAME = source._STG_FILE_NAME,
        target._STG_FILE_LOAD_TS = source._STG_FILE_LOAD_TS,
        target._STG_FILE_MD5 = source._STG_FILE_MD5,
        target._COPY_DATA_TS = source._COPY_DATA_TS
WHEN NOT MATCHED THEN
    INSERT (
        CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        CREATED_DT,
        MODIFIED_DT,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    )
    VALUES (
        source.CUSTOMER_ID,
        source.NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.PREFERENCES,
        source.CREATED_DT,
        source.MODIFIED_DT,
        source._STG_FILE_NAME,
        source._STG_FILE_LOAD_TS,
        source._STG_FILE_MD5,
        source._COPY_DATA_TS
    );

/*------------------------------------------------------------------------------
    PART 6: CLEAN TO CONSUMPTION LAYER TRANSFORMATION (SCD TYPE 2)
------------------------------------------------------------------------------*/

-- Transform and load data from clean to consumption layer
-- Implements SCD Type 2 (historical tracking) with effective dates
MERGE INTO
    CONSUMPTION_SCH.CUSTOMER_DIM AS target
USING
    CLEAN_SCH.CUSTOMER_STM AS source
ON
    target.CUSTOMER_ID = source.CUSTOMER_ID AND
    target.NAME = source.NAME AND
    target.MOBILE = source.MOBILE AND
    target.EMAIL = source.EMAIL AND
    target.LOGIN_BY_USING = source.LOGIN_BY_USING AND
    target.GENDER = source.GENDER AND
    target.DOB = source.DOB AND
    target.ANNIVERSARY = source.ANNIVERSARY AND
    target.PREFERENCES = source.PREFERENCES
WHEN MATCHED
    AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET
        target.EFF_END_DATE = CURRENT_TIMESTAMP(),
        target.IS_CURRENT = FALSE
WHEN NOT MATCHED
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    INSERT (
        CUSTOMER_HK,
        CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(
            SHA1_hex(
                CONCAT(
                    source.CUSTOMER_ID,
                    source.NAME,
                    source.MOBILE,
                    source.EMAIL,
                    source.LOGIN_BY_USING,
                    source.GENDER,
                    source.DOB,
                    source.ANNIVERSARY,
                    source.PREFERENCES
                )
            )
        ),
        source.CUSTOMER_ID,
        source.NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.PREFERENCES,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
WHEN NOT MATCHED
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    INSERT (
        CUSTOMER_HK,
        CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(
            SHA1_hex(
                CONCAT(
                    source.CUSTOMER_ID,
                    source.NAME,
                    source.MOBILE,
                    source.EMAIL,
                    source.LOGIN_BY_USING,
                    source.GENDER,
                    source.DOB,
                    source.ANNIVERSARY,
                    source.PREFERENCES
                )
            )
        ),
        source.CUSTOMER_ID,
        source.NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.PREFERENCES,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );

/*------------------------------------------------------------------------------
    PART 7: INITIAL DATA LOADING
------------------------------------------------------------------------------*/

-- Load initial customer data from CSV files
-- This is the first data load before any delta processing
copy into stage_sch.customer (
    customerid,
    name,
    mobile,
    email,
    loginbyusing,
    gender,
    dob,
    anniversary,
    preferences,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as customerid,
        t.$2::text as name,
        t.$3::text as mobile,
        t.$4::text as email,
        t.$5::text as loginbyusing,
        t.$6::text as gender,
        t.$7::text as dob,
        t.$8::text as anniversary,
        t.$9::text as preferences,
        t.$10::text as createddate,
        t.$11::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/initial/customer/customers-initial.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

/*------------------------------------------------------------------------------
    PART 8: DELTA DATA LOADING
------------------------------------------------------------------------------*/

-- List files in delta directory to check for new data
list @csv_stg/delta/customer/;

-- Load delta customer data (new and updated records)
copy into stage_sch.customer (
    customerid,
    name,
    mobile,
    email,
    loginbyusing,
    gender,
    dob,
    anniversary,
    preferences,
    createddate,
    modifieddate,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
from (
    select
        t.$1::text as customerid,
        t.$2::text as name,
        t.$3::text as mobile,
        t.$4::text as email,
        t.$5::text as loginbyusing,
        t.$6::text as gender,
        t.$7::text as dob,
        t.$8::text as anniversary,
        t.$9::text as preferences,
        t.$10::text as createddate,
        t.$11::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/delta/customer/day-02-insert-update.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

/*------------------------------------------------------------------------------
    DEBUGGING AND BACKUP OPERATIONS
------------------------------------------------------------------------------*/

-- Create backup of stream data for debugging and analysis
CREATE TABLE common.dump_customer_stm AS
SELECT * FROM stage_sch.customer_stm;

/*------------------------------------------------------------------------------
    CLEAN UP STATEMENTS
    These statements are used to clean up existing objects for re-execution
------------------------------------------------------------------------------*/

-- Drop stage schema objects (commented out for safety)
-- DROP TABLE stage_sch.customer;

/*------------------------------------------------------------------------------
    DIAGNOSTIC QUERIES
    These queries are used throughout the script for validation and debugging
------------------------------------------------------------------------------*/

-- Stage layer validation queries
-- select * from stage_sch.customer;
-- select * from stage_sch.customer_stm;

-- Clean layer validation queries
-- select * from clean_sch.customer;
-- select * from clean_sch.customer_stm;