use role sysadmin;
use warehouse adhoc_wh;
use database sandbox;
use schema stage_sch;

--- Query the data from external location
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

--- Query the data from external location by adding audit columns
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
    current_date as _copy_data_dt,
    current_timestamp as _copy_data_ts
from @stage_sch.csv_stg/initial/location/location-5rows.csv
(file_format => 'stage_sch.csv_file_format') t;



create table stage_sch.location (
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createdate text,
    modifieddate text,
    --- audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the lication stage/raw table where data will be copied from internal stage using copy command. This is as-is data representation from the source location. All the columns are text datatype except the audit columns that are added for traceability.';

DROP TABLE IF EXISTS stage_sch.location;