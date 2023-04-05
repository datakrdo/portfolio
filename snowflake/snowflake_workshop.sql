
/********
Script Para el taller de Snowflake

Ultima actualización Julio 2022


*********/


-- Creamos una base de datos
-- Puede hacerse a través de la interfaz gráfica o de la línea de comando
-- Let's create a databaase which we can do from the UI or the command line

use role sysadmin;

CREATE OR REPLACE DATABASE Citibike COMMENT = 'Base de datos de prueba Citibike';

-- Create a Warehouse in case it has not been created yet

CREATE OR REPLACE WAREHOUSE COMPUTE_WH 
WITH WAREHOUSE_SIZE = 'MEDIUM' WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 60 
AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 2 
SCALING_POLICY = 'STANDARD';

-- This SQL file is for the Hands On Lab Guide for the 30-day free Snowflake trial account
-- The numbers below correspond to the sections of the Lab Guide in which SQL is to be run in a Snowflake worksheet

-- Modules 1 and 2 of the Lab Guide have no SQL to be run


/* *********************************************************************************** 
   *** MODULE 3  ********************************************************************* 
 *********************************************************************************** */

-- 3.1.3

use role sysadmin;
use warehouse compute_wh;
use database citibike;
use schema public;

-- 3.1.4

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

-- Create a stage against S3

/*  Crear el stage */

 CREATE STAGE "CITIBIKE"."PUBLIC".citibike_trips 
 URL = 's3://snowflake-workshop-lab/citibike-trips-csv' 
 COMMENT = 'Stage Externo para el cargado de datos de Citibike';


-- 3.2.4

list @citibike_trips;

-- how much data is this?
select floor(sum($2)/power(1024, 3),1) total_compressed_storage_gb,
    floor(avg($2)/power(1024, 2),1) avg_file_size_mb,
    count(*) as num_files
  from table(result_scan(last_query_id()));


-- 3.3 Creando un File Format
---    Let's create a File Format

/*  */
CREATE FILE FORMAT "CITIBIKE"."PUBLIC".CSV 
TYPE = 'CSV' COMPRESSION = 'AUTO' 
FIELD_DELIMITER = ',' 
RECORD_DELIMITER = '\n' 
SKIP_HEADER = 0 
FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
TRIM_SPACE = FALSE 
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE 
ESCAPE = 'NONE' 
ESCAPE_UNENCLOSED_FIELD = '\134' 
DATE_FORMAT = 'AUTO' 
TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('') 
COMMENT = 'Formato de archivo para el cargado de los datos de Citibike';



-- Validando los datos en el stage, que conformen con el formato del archivo que acabamos de crear
-- Let's validate the data in stage, to make sure it conforms to the file format we just defined

select $1, $2, $3, $4, $5
from @citibike_trips/trips_2013_0_0_0.csv.gz
(file_format => CSV)
limit 10;


/* ***********************************************************************************
 *** MODULE 4  ********************************************************************* 
       Let's load some data
   *********************************************************************************** */

--4.2.2

copy into trips 
from @citibike_trips/trips
file_format=CSV;

select count(*) from trips;

-- 4.2.4

truncate table trips;

-- Cambiamos el tamaño del warehouse a grande y veamos la diferencia en el tiempo del cargado de los datos
-- Let's change the size of the warehouse to large and see the difference in time when loading the data

ALTER WAREHOUSE "COMPUTE_WH" SET WAREHOUSE_SIZE = 'LARGE' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- 4.2.7

copy into trips 
from @citibike_trips/trips
file_format=CSV;

select count(*) from trips;

alter warehouse compute_wh set warehouse_size = 'xsmall';

/* *********************************************************************************** 
   *** MODULE 5  ********************************************************************* 
      Utilizando el cache de datos 
           Let's use the cache of data
 *********************************************************************************** */

-- 5.1.2

select * from trips limit 20;

-- 5.1.3

select 
	date_trunc('hour', starttime) as "date",
	count(*) as "num trips",
	avg(tripduration)/60 as "avg duration (mins)", 
	avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)",
	60*("avg distance (km)"/"avg duration (mins)") "Speed km/h"
from trips
group by 1 order by 2 desc;

-- 5.1.4

select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)", 
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)", 
60*("avg distance (km)"/"avg duration (mins)") "Speed km/h"
from trips
group by 1 order by 1;

-- 5.1.5

select
    dayname(starttime) as "day of week",
    count(*) as "num trips"
from trips
group by 1 order by 2 desc;

-- 5.2.1 --- Cloning a database table and a full database

--- We can clone tables, schemas, and database

--drop database citibike_dev;

create table trips_dev clone trips;

create or replace database citibike_dev clone citibike;

/* *********************************************************************************** 
   *** MODULE 6  ********************************************************************* 
   *** Trabajando con datos tipo JSON												
       Working with JSON data
  *********************************************************************************** */

-- 6.1.1

create or replace database weather;

-- 6.1.2

use role sysadmin;
use warehouse compute_wh;
use database weather;
use schema public;

-- 6.1.3

create or replace table json_weather_data (v variant);

-- 6.2.1

create stage nyc_weather
url = 's3://snowflake-workshop-lab/weather-nyc';

-- 6.2.2 

list @nyc_weather;

-- 6.3.1

copy into json_weather_data 
from @nyc_weather 
file_format = (type=json);

-- 6.3.2

select * from json_weather_data limit 10;

-- 6.4.1

create or replace view json_weather_data_view as
select
  v:time::timestamp as observation_time,
  v:city.id::int as city_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:city.coord.lat::float as city_lat,
  v:city.coord.lon::float as city_lon,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:weather[0].main::string as weather,
  v:weather[0].description::string as weather_desc,
  v:weather[0].icon::string as weather_icon,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from json_weather_data
where city_id = 5128638;

--create table weather_final as
--select *
--from jason_weather_data_view;

-- 6.4.4

select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01' 
limit 20;

-- 6.5.1

select weather as conditions
    ,count(*) as num_trips
from citibike.public.trips 
left outer join json_weather_data_view
    on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;


/* *********************************************************************************** 
   *** MODULE 7  ********************************************************************* 
   *********************************************************************************** */

-- 7.1.1 -- Continuous Data Protection

drop table json_weather_data;

-- 7.1.2

Select * from json_weather_data limit 10;

-- 7.1.3

undrop table json_weather_data;

-- 7.2.1 --- Time Travel

use role sysadmin;
use warehouse compute_wh;
use database citibike;
use schema public;


-- 7.2.2

update trips set start_station_name = 'oops';

-- 7.2.3

select 
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


-- 7.2.4

set query_id = 
(select query_id from 
table(information_schema.query_history_by_session (result_limit=>5)) 
where query_text like 'update%' order by start_time limit 1);

select $query_id;

-- 7.2.5
create or replace table trips as
(select * from trips before (statement => $query_id));
        
-- 7.2.6

select 
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


/* *********************************************************************************** 
   *** MODULE 8  ********************************************************************* 
   *********************************************************************************** */

-- 8.1.1

use role accountadmin; 

-- 8.1.3 (NOTE - enter your unique user name into the second row below)

create or replace role junior_dba;
-- grant role junior_daba to user <change to your user name here>;
grant role junior_dba to user admin;

-- 8.1.4

use role junior_dba;

-- 8.1.6 -- Assign different privileges to the new role

use role accountadmin;
grant usage on database citibike to role junior_dba;
grant usage on schema public to role junior_dba;
grant select on all tables in schema citibike.public to role junior_dba;
grant usage on database weather to role junior_dba;
grant usage on warehouse compute_wh to role junior_dba;


-- create a masking policy
create or replace masking policy membership_mask as (val string) returns string ->
  case
    when current_role() in ('JUNIOR_DBA') then '*******'
    else val
  end;

alter table trips modify column membership_type set masking policy membership_mask; 

select membership_type, count(1)
from trips
group by membership_type;

-- 8.1.7 -- let's query the data with the new role

use role junior_dba;

select membership_type, count(1)
from trips
group by membership_type;

select * from trips
limit 100;

use role accountadmin;

select membership_type, count(1)
from trips
group by membership_type;


-- Using Data from the Marketplace
-- Go into the Data Marketplace
-- Seaarch for COVID
-- Share Starschema data
-- Name the database Covid_19


select * from "Covid_19".public.jhu_covid_19
order by date desc
limit 10;


-- END Script
-- OPTIONAL reset script to remove all objects created in the lab

use role accountadmin;
use warehouse compute_wh;
use database weather;
use schema public;



drop share if exists trips_share;
drop database if exists citibike;
drop database if exists citibike_dev;
drop database if exists weather;
drop warehouse if exists compute_wh;
drop role if exists junior_dba;




