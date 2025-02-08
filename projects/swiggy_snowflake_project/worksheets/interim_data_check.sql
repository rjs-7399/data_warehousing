use role sysadmin;
use sandbox;

--- Loading and validating the data
--- root location
list @stage_sch.csv_stg;

--- Loading and validating the data in initial partition
list @stage_sch.csv_stg/initial;

--- Loading and validating the data in delta location
list @stage_sch.csv_stg/delta;


--- SQL Command to check the data in stage location, without querying the actual table
select
    t.$1::text as locationid,
    t.$2::text as city,
    t.$3::text as state,
    t.$4::text as zipcode,
    t.$5::text as activeflag,
    t.$6::text as createdate,
    t.$7::text as modifieddate
from @stage_sch.csv_stg/initial/location/
(file_format => 'stage_sch.csv_file_format') t;
