# AWS EMR transform queries

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

fact_cmplnt_transform= ("""
    WITH cte as
    (SELECT *,
           IF (year(to_timestamp(string(CMPLNT_FR_DT), 'MM/dd/yyyy')) < 1678, NULL, to_timestamp(string(CMPLNT_FR_DT), 'MM/dd/yyyy')) AS CMPLNT_DATE_TIMESTAMP,
           IF (year(to_timestamp(concat(string(CMPLNT_FR_DT), ' ', string(CMPLNT_FR_TM)), 'MM/dd/yyyy HH:mm:ss')) < 1678, NULL, to_timestamp(concat(string(CMPLNT_FR_DT), ' ', string(CMPLNT_FR_TM)), 'MM/dd/yyyy HH:mm:ss')) AS CMPLNT_TIME_TIMESTAMP
     FROM fact_cmplnt
    )
    SELECT 
           CMPLNT_NUM as cmplnt_key,
           IF (CMPLNT_DATE_TIMESTAMP IS NULL, -1, INT(date_format(CMPLNT_DATE_TIMESTAMP, 'yyyyMMdd'))) AS date_key,
           IF (CMPLNT_TIME_TIMESTAMP IS NULL OR CMPLNT_TIME_TIMESTAMP IS NULL, -1, INT(date_format(CMPLNT_TIME_TIMESTAMP, 'HHmm'))) AS time_key
    FROM cte;
""")

dim_weather_transform= ("""
    SELECT date_format(to_date(DATE, 'YYYY-MM-DD'), 'yyyyMMdd') AS date_key,
           regexp_replace(MAX(HourlyDryBulbTemperature), '[^0-9]+', '') AS high_temp, --fix random non-numerica data
           regexp_replace(MIN(HourlyDryBulbTemperature), '[^0-9]+', '') AS low_temp --fix random non-numerica data
    FROM dim_weather
    GROUP BY date_format(to_date(DATE, 'YYYY-MM-DD'), 'yyyyMMdd')
    ORDER BY date_key DESC;
""")

# Test queries

fact_cmplnt_test ("""
    SELECT COUNT(1) as result
    FROM fact_cmplnt;
""")

dim_weather_test ("""
    SELECT COUNT(1) as result
    FROM dim_weather;
""")

transform_table_queries = {"dim_date": {"source_data": "nypd-complaint.csv", "query": dim_date_transform},
                           "dim_time": {"source_data": "nypd-complaint.csv", "query": dim_time_transform},
                           "fact_cmplnt": {"source_data": "nypd-complaint.csv", "query": fact_cmplnt_transform},
                           "dim_weather": {"source_data": "nyc-weather.csv", "query": dim_weather_transform}
                           }

test_table_queries = {"fact_cmplnt": {"source_data": "fact_cmplnt.csv", "query": fact_cmplnt_test, "expected_value": 7309655},
                      "dim_weather": {"source_data": "dim_weather.csv", "query": dim_weather_test, "expected_value": 5303}
                     }
