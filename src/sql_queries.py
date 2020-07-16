import configparser

# CONFIG
config = configparser.ConfigParser()
config.read_file(open('/tmp/redshift.cfg'))
DWH_IAM_ROLE = config.get("DWH", "IAM_ROLE_ARN")

# DROP TABLES

dim_time_table_drop = "DROP TABLE IF EXISTS dim_time"
dim_date_table_drop = "DROP TABLE IF EXISTS dim_date"
fact_cmplnt_table_drop = "DROP TABLE IF EXISTS fact_cmplnt"

# CREATE TABLES

dim_time_transform= ("""
    WITH cte as
    (SELECT *,
           IF (year(to_timestamp(concat(string(CMPLNT_FR_DT), ' ', string(CMPLNT_FR_TM)), 'MM/dd/yyyy HH:mm:ss')) < 1678, NULL, to_timestamp(concat(string(CMPLNT_FR_DT), ' ', string(CMPLNT_FR_TM)), 'MM/dd/yyyy HH:mm:ss')) AS CMPLNT_TIMESTAMP
     FROM dim_time
    )
    SELECT 
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_TIMESTAMP IS NULL, -1, INT(date_format(CMPLNT_TIMESTAMP, 'HHmm'))) AS key,
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', '*** No Time Given ***', date_format(CMPLNT_TIMESTAMP, 'h:mm a')) AS time,
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', -1, date_format(CMPLNT_TIMESTAMP, 'k')) AS clock_hour_of_day,
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', -1, date_format(CMPLNT_TIMESTAMP, 'K')) AS hour_of_am_pm,
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', '*** No Time Given ***', date_format(CMPLNT_TIMESTAMP, 'a')) AS am_pm_of_day,
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', '*** No Time Given ***', date_format(CMPLNT_TIMESTAMP, 'HH:mm')) AS military_time,
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', -1, date_format(CMPLNT_TIMESTAMP, 'H')) AS hour_of_day,
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', -1, date_format(CMPLNT_TIMESTAMP, 'm')) AS minute_of_hour,
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', -1, INT(date_format(CMPLNT_TIMESTAMP, 'H')*60+date_format(CMPLNT_TIMESTAMP, 'm'))) AS minute_of_day
    FROM cte
    GROUP BY 
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_TIMESTAMP IS NULL, -1, INT(date_format(CMPLNT_TIMESTAMP, 'HHmm'))),
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', '*** No Time Given ***', date_format(CMPLNT_TIMESTAMP, 'h:mm a')),
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', -1, date_format(CMPLNT_TIMESTAMP, 'k')),
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', -1, date_format(CMPLNT_TIMESTAMP, 'K')),
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', '*** No Time Given ***', date_format(CMPLNT_TIMESTAMP, 'a')),
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', '*** No Time Given ***', date_format(CMPLNT_TIMESTAMP, 'HH:mm')),
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', -1, date_format(CMPLNT_TIMESTAMP, 'H')),
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', -1, date_format(CMPLNT_TIMESTAMP, 'm')),
           IF (CMPLNT_TIMESTAMP IS NULL OR CMPLNT_FR_TM IS NULL OR CMPLNT_FR_TM = '', -1, INT(date_format(CMPLNT_TIMESTAMP, 'H')*60+date_format(CMPLNT_TIMESTAMP, 'm')))
    ORDER BY key;
""")

dim_date_transform= ("""
    WITH cte as
    (SELECT *,
           IF (year(to_timestamp(string(CMPLNT_FR_DT), 'MM/dd/yyyy')) < 1678, NULL, to_timestamp(string(CMPLNT_FR_DT), 'MM/dd/yyyy')) AS CMPLNT_TIMESTAMP
     FROM dim_date
    )
    SELECT 
           IF (CMPLNT_TIMESTAMP IS NULL, -1, INT(date_format(CMPLNT_TIMESTAMP, 'yyyyMMdd'))) AS key,
           DATE(IF (CMPLNT_FR_DT IS NULL OR CMPLNT_TIMESTAMP IS NULL, '1900-01-01', CMPLNT_TIMESTAMP)) AS date,
           IF (INT(day(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(day(CMPLNT_TIMESTAMP))) AS day,
           IF (INT(dayofweek(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(dayofweek(CMPLNT_TIMESTAMP))) AS day_of_week,
           IF (date_format(CMPLNT_TIMESTAMP, 'EEEE') IS NULL, '*** No Date Given ***', date_format(CMPLNT_TIMESTAMP, 'EEEE')) AS day_of_week_name,
           IF (INT(dayofyear(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(dayofyear(CMPLNT_TIMESTAMP))) AS day_of_year,
           IF (INT(dayofmonth(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(dayofmonth(CMPLNT_TIMESTAMP))) AS day_of_month,
           IF (INT(month(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(month(CMPLNT_TIMESTAMP))) AS month,
           IF (date_format(CMPLNT_TIMESTAMP, 'MMMM') IS NULL, '*** No Date Given ***', date_format(CMPLNT_TIMESTAMP, 'MMMM')) AS month_name,
           IF (INT(weekday(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(weekday(CMPLNT_TIMESTAMP))) AS weekday,
           IF (INT(weekofyear(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(weekofyear(CMPLNT_TIMESTAMP))) AS week_of_year,
           IF (INT(quarter(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(quarter(CMPLNT_TIMESTAMP))) AS quarter
    FROM cte
    GROUP BY 
           IF (CMPLNT_TIMESTAMP IS NULL, -1, INT(date_format(CMPLNT_TIMESTAMP, 'yyyyMMdd'))),
           DATE(IF (CMPLNT_FR_DT IS NULL OR CMPLNT_TIMESTAMP IS NULL, '1900-01-01', CMPLNT_TIMESTAMP)),
           IF (INT(day(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(day(CMPLNT_TIMESTAMP))),
           IF (INT(dayofweek(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(dayofweek(CMPLNT_TIMESTAMP))),
           IF (date_format(CMPLNT_TIMESTAMP, 'EEEE') IS NULL, '*** No Date Given ***', date_format(CMPLNT_TIMESTAMP, 'EEEE')),
           IF (INT(dayofyear(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(dayofyear(CMPLNT_TIMESTAMP))),
           IF (INT(dayofmonth(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(dayofmonth(CMPLNT_TIMESTAMP))),
           IF (INT(month(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(month(CMPLNT_TIMESTAMP))),
           IF (date_format(CMPLNT_TIMESTAMP, 'MMMM') IS NULL, '*** No Date Given ***', date_format(CMPLNT_TIMESTAMP, 'MMMM')),
           IF (INT(weekday(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(weekday(CMPLNT_TIMESTAMP))),
           IF (INT(weekofyear(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(weekofyear(CMPLNT_TIMESTAMP))),
           IF (INT(quarter(CMPLNT_TIMESTAMP)) IS NULL, -1, INT(quarter(CMPLNT_TIMESTAMP)))
    ORDER BY key;
""")

cmplnt_table_create = ("""
CREATE TABLE cmplnt (
    cmplnt_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR DISTKEY SORTKEY,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR NULL,
    user_agent VARCHAR NULL
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
FROM 's3://nypd-complaint/dim_time.csv'
IAM_ROLE '{}'
REGION 'us-west-2'
CSV
IGNOREHEADER 1;
""").format(DWH_IAM_ROLE)


dim_date_copy = ("""
COPY dim_date
FROM 's3://nypd-complaint/dim_date.csv'
IAM_ROLE '{}'
REGION 'us-west-2' 
CSV
IGNOREHEADER 1;
""").format(DWH_IAM_ROLE)

# QUERY LISTS

create_table_queries = [dim_time_table_create, dim_date_table_create]
drop_table_queries = [dim_time_table_drop, dim_date_table_drop, fact_cmplnt_table_drop]
copy_table_queries = [dim_time_copy, dim_date_copy]
transform_table_queries = [dim_date_transform, dim_time_transform]
