import configparser

# CONFIG
config = configparser.ConfigParser()
config.read_file(open('/tmp/redshift.cfg'))
DWH_IAM_ROLE = config.get("DWH", "IAM_ROLE_ARN")

# DROP TABLES

dim_time_table_drop = "DROP TABLE IF EXISTS dim_time"
dim_date_table_drop = "DROP TABLE IF EXISTS dim_date"
dim_weather_table_drop = "DROP TABLE IF EXISTS dim_weather"
fact_cmplnt_table_drop = "DROP TABLE IF EXISTS fact_cmplnt"

# CREATE TABLES

fact_cmplnt_table_create = ("""
CREATE TABLE fact_cmplnt (
    cmplnt_key BIGINT PRIMARY KEY,
    date_key INT NOT NULL,
    time_key SMALLINT NOT NULL
    );
""")

dim_weather_table_create = ("""
CREATE TABLE dim_weather (
    date_key INT PRIMARY KEY,
    high_temp SMALLINT,
    low_temp SMALLINT
    );
""")

dim_time_table_create = ("""
CREATE TABLE dim_time (
    key SMALLINT PRIMARY KEY,
    time VARCHAR NOT NULL,
    clock_hour_of_day SMALLINT NOT NULL,
    hour_of_am_pm SMALLINT NOT NULL,
    am_pm_of_day VARCHAR NOT NULL,
    military_time VARCHAR NOT NULL,
    hour_of_day SMALLINT NOT NULL,
    minute_of_hour SMALLINT NOT NULL,
    minute_of_day SMALLINT NOT NULL
    ) diststyle all;
""")

dim_date_table_create = ("""
CREATE TABLE dim_date (
    key INT PRIMARY KEY,
    date DATE NOT NULL,
    day SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    day_of_week_name VARCHAR NOT NULL,
    day_of_year SMALLINT NOT NULL,
    day_of_month SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    month_name VARCHAR NOT NULL,
    weekday SMALLINT NOT NULL,
    week_of_year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL
    ) diststyle all;
""")

# Load dimension & fact tables

dim_time_copy = ("""
COPY dim_time
FROM 's3://nypd-complaint/data/transform/dim_time.csv'
IAM_ROLE '{}'
REGION 'us-west-2'
CSV
IGNOREHEADER 1;
""").format(DWH_IAM_ROLE)

dim_weather_copy = ("""
COPY dim_weather
FROM 's3://nypd-complaint/data/transform/dim_weather.csv'
IAM_ROLE '{}'
REGION 'us-west-2'
CSV
IGNOREHEADER 1;
""").format(DWH_IAM_ROLE)

dim_date_copy = ("""
COPY dim_date
FROM 's3://nypd-complaint/data/transform/dim_date.csv'
IAM_ROLE '{}'
REGION 'us-west-2' 
CSV
IGNOREHEADER 1;
""").format(DWH_IAM_ROLE)

fact_cmplnt_copy = ("""
COPY fact_cmplnt
FROM 's3://nypd-complaint/data/transform/fact_cmplnt.csv'
IAM_ROLE '{}'
REGION 'us-west-2' 
CSV
IGNOREHEADER 1;
""").format(DWH_IAM_ROLE)

# QUERY LISTS

create_table_queries = [dim_time_table_create, dim_date_table_create, fact_cmplnt_table_create, dim_weather_table_create]
drop_table_queries = [dim_time_table_drop, dim_date_table_drop, fact_cmplnt_table_drop, dim_weather_table_drop]
copy_table_queries = [dim_time_copy, dim_date_copy, fact_cmplnt_copy, dim_weather_copy]
