import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, types
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import subprocess
import sys
import pandas as pd
import numpy as np
import boto3


def create_spark_session(env="local"):
    """
    Create SparkSession object

    Parameters:
    env (string): Environment (i.e. local or aws). Default: local

    Returns:
    spark (pyspark.sql.SparkSession): Spark session
    """

    config = configparser.ConfigParser()
    if env == "local":
        config.read('/tmp/local.cfg')
        aws_s3_uri = config.get("AWS", "AWS_S3_URI")
        aws_access_key = config.get("AWS", "AWS_ACCESS_KEY_ID")
        aws_secret_key = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

    # Set Spark connections
    if env == "local":
        # Adapted from https://www.jitsejan.com/setting-up-spark-with-minio-as-object-storage.html
        conf = (
            SparkConf()
            .setAppName("NYPD complaint Analysis")
            .set("spark.hadoop.fs.s3a.endpoint", aws_s3_uri)
            .set("spark.hadoop.fs.s3a.access.key", aws_access_key)
            .set("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
            .set("spark.hadoop.fs.s3a.path.style.access", True)
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
        sc = SparkContext(conf=conf).getOrCreate()
        sqlContext = SQLContext(sc)
        spark = sqlContext.sparkSession
    else:
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .appName("NYPD Complaint Analysis") \
            .getOrCreate()

    return spark

def get_nypd_complaint_record_cnt(spark, env="local", aws_access_key="", aws_secret_key=""):
    """
    NYPD complaint analysis

    Parameters:
    spark (pyspark.sql.SparkSession): Spark session
    env (string): Environment (i.e. local or aws). Default: local
    aws_access_key (string): AWS access key
    aws_secret_key (string): AWS secret key

    Returns:
    None
    """

    config = configparser.ConfigParser()
    if env == "local":
        config.read('/tmp/local.cfg')
        aws_s3_uri = config.get("AWS", "AWS_S3_URI")
        aws_access_key = config.get("AWS", "AWS_ACCESS_KEY_ID")
        aws_secret_key = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

    df = spark.read.csv("s3a://nypd-complaint/nypd-complaint.csv", inferSchema = True, header = True)
#   df.printSchema()

    df.createOrReplaceTempView("nypd_complaint_analysis")

    record_cnt = spark.sql("""
    SELECT COUNT(1) as cnt
    FROM nypd_complaint_analysis;
    """)

#   print(record_cnt.head())

    cmplnt_cnt_by_fr_dt = spark.sql("""
    SELECT CMPLNT_FR_DT, 
           COUNT(1) AS CNT
    FROM nypd_complaint_analysis
    GROUP BY CMPLNT_FR_DT
    ORDER BY CNT DESC;
    """)

#   i = 1
#   for index, row in cmplnt_cnt_by_fr_dt.select("*").toPandas().iterrows():
#       if i > 10:
#           break
#       print(row["CMPLNT_FR_DT"] + " " + str(row["CNT"]))
#       i += 1

    df = spark.read.csv("s3a://nypd-complaint/nyc-weather.csv", inferSchema = True, header = True)
#   df.printSchema()

    df.createOrReplaceTempView("nyc_weather")

    record_cnt = spark.sql("""
    SELECT COUNT(1) as cnt
    FROM nyc_weather;
    """)
#   print(record_cnt.head())

    temperature_analysis_by_date = spark.sql("""
    SELECT to_date(DATE, 'YYYY-MM-DD') AS date, 
           MAX(HourlyDryBulbTemperature) AS HighTemp,
           MIN(HourlyDryBulbTemperature) AS LowTemp 
    FROM nyc_weather 
    GROUP BY to_date(DATE, 'YYYY-MM-DD') 
    ORDER BY date DESC;
    """)

    temperature_analysis_by_date.toPandas().to_csv("/tmp/temperature_analysis_by_date.csv", header=True)
    if env == "local":
        s3 = boto3.resource('s3',
             endpoint_url=aws_s3_uri,
             aws_access_key_id=aws_access_key,
             aws_secret_access_key=aws_secret_key)
    else:
        s3 = boto3.resource('s3',
             aws_access_key_id=aws_access_key,
             aws_secret_access_key=aws_secret_key)

    s3.Bucket("nypd-complaint").upload_file("/tmp/temperature_analysis_by_date.csv", "temperature_analysis_by_date.csv")

def main():
    """
    Analyze NYPD complaints

    Parameters:
    env (string): Environment (i.e. local or aws). Default: local
    aws_access_key (string): AWS access key
    aws_secret_key (string): AWS secret key

    Returns:
    None
    """

    env = sys.argv[1]
    aws_access_key = sys.argv[2]
    aws_secret_key = sys.argv[3]
    spark = create_spark_session(env)
    get_nypd_complaint_record_cnt(spark, env, aws_access_key, aws_secret_key)

if __name__ == "__main__":
    main()
