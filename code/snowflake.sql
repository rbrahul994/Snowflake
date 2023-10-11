-- create database and table 

create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);


-- create external stage

-- add the external stage and from where the data is ingested

list @citibike_trips;  -- shows the files in the s3 bucket


--create file format

create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data to snowflake';

--verify file format is created       

show file formats in database citibike;

copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

truncate table trips;

--verify table is clear
select * from trips limit 10;

--change warehouse size from small to large (4x)
alter warehouse compute_wh set warehouse_size='large';

--load data with large warehouse
show warehouses;

copy into trips from @citibike_trips
file_format=CSV;

# make sure of the worksheet settings  

### Role: SYSADMIN Warehouse: COMPUTE_WH Database: CITIBIKE Schema = PUBLIC ###


copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;


# check the data migration with larger warehouse

truncate table trips;

--verify table is clear
select * from trips limit 10;

--load data with large warehouse
show warehouses;

copy into trips from @citibike_trips
file_format=CSV;

select * from trips limit 20;

select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

-- Use the Result Cache

-- Snowflake has a result cache that holds the results of every query executed in the past 24 hours. 
-- These are available across warehouses, so query results returned to one user are available to any other user on the system who executes the same query, 
-- provided the underlying data has not changed. Not only do these repeated queries return extremely fast, but they also use no compute credits.


-- Zero-Copy Cloning A massive benefit of zero-copy cloning is that the underlying data is not copied. 
-- Only the metadata and pointers to the underlying data change. Hence, clones are "zero-copy" and 
-- storage requirements are not doubled when the data is cloned. Most data warehouses cannot do this, but for Snowflake it is easy!

create table trips_dev clone trips;

-- SEMI-STRUCTURED DATA Snowflake can easily load and query semi-structured data such as JSON, Parquet, 
-- or Avro without transformation. This is a key Snowflake feature because an increasing amount of business-relevant 
-- data being generated today is semi-structured, and many traditional data warehouses cannot easily load and query such data. 


create database weather;

use role sysadmin;

use warehouse compute_wh;

use database weather;

use schema public;

create table json_weather_data (v variant);

-- Note that Snowflake has a special column data type called VARIANT that allows storing the entire JSON object as a single row and eventually query the object directly.

create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';

list @nyc_weather;

copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);

select * from json_weather_data limit 10;

-- Views & Materialized Views 

-- A view allows the result of a query to be accessed as if it were a table. 
-- Views can help present data to end users in a cleaner manner, limit what end users can view in a source table, 
-- and write more modular SQL. 

-- Snowflake also supports materialized views in which the query results are stored 
-- as though the results are a table. This allows faster access, but requires storage space. 
-- Materialized views can be created and queried if you are using Snowflake Enterprise Edition (or higher).

create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data
where
    station_id = '72502';

-- when dealing with nested json use the "." notation

select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;

select weather_conditions as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;

-- Drop and Undrop a Table

drop table json_weather_data;

select * from json_weather_data limit 10;

undrop table json_weather_data;

--verify table is undropped

select * from json_weather_data limit 10;


-- Roll Back a Table

use role sysadmin;

use warehouse compute_wh;

use database citibike;

use schema public;

update trips set start_station_name = 'oops';

select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time desc limit 1);

create or replace table trips as
(select * from trips before (statement => $query_id));

select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;



-- Role-Based Access Control Snowflake offers very powerful and granular access control 
-- that dictates the objects and functionality a user can access, as well as 
-- the level of access they have.

use role accountadmin;

create role junior_dba;

grant role junior_dba to user rahul;

-- Note SYSADMIN, will fail due to insufficient privileges

use role junior_dba;


use role accountadmin;

grant usage on warehouse compute_wh to role junior_dba;

grant usage on database citibike to role junior_dba;

grant usage on database weather to role junior_dba;

use role junior_dba;

use warehouse compute_wh;

-- Reset the snowflake account

use role accountadmin;

drop share if exists snowflake_shared_data;

drop database if exists citibike;

drop database if exists weather;

drop warehouse if exists compute_wh;

drop role if exists junior_dba;

