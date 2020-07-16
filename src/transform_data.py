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
from sql_queries import *


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

def create_csv_for_redshift(spark, env="local", aws_access_key="", aws_secret_key="", dataset="", s3_uri="", sql=""):
    """
    NYPD complaint analysis

    Parameters:
    spark (pyspark.sql.SparkSession): Spark session
    env (string): Environment (i.e. local or aws). Default: local
    aws_access_key (string): AWS access key
    aws_secret_key (string): AWS secret key
    dataset (string): Dataset to transform & write to CSV
    s3_uri (string): S3 URI of source data to transform
    sql (string): SQL statement to transform dataset

    Returns:
    None
    """

    if env == "local":
        config = configparser.ConfigParser()
        config.read('/tmp/local.cfg')
        aws_s3_uri = config.get("AWS", "AWS_S3_URI")
        aws_access_key = config.get("AWS", "AWS_ACCESS_KEY_ID")
        aws_secret_key = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

    df = spark.read.csv(s3_uri, inferSchema = True, header = True)

    df.createOrReplaceTempView(dataset)

    result = spark.sql(sql)

    csv_file = "/tmp/" + dataset + ".csv"
    s3_key = dataset + ".csv"

    # Write Pandas dataframe to create single CSV file
    # Utilizing Spark's csv write function creates many files 
    result.toPandas().to_csv(csv_file, header=True, index=False)

    # Set up boto3 S3 resource based on execution environment
    if env == "local":
        s3 = boto3.resource('s3',
             endpoint_url=aws_s3_uri,
             aws_access_key_id=aws_access_key,
             aws_secret_access_key=aws_secret_key)
    else:
        s3 = boto3.resource('s3',
             aws_access_key_id=aws_access_key,
             aws_secret_access_key=aws_secret_key)

    # Write CSV file to S3
    s3.Bucket("nypd-complaint").upload_file(csv_file, s3_key)

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

    for dataset, query in transform_sql_queries.items():
        create_csv_for_redshift(spark, 
                                env, 
                                aws_access_key, 
                                aws_secret_key,
                                dataset=dataset,
                                s3_uri="s3a://nypd-complaint/raw/nypd-complaint.csv",
                                sql=query
                                )

#   # Transform & create NYPD complaint CSV file for redshift
#   create_csv_for_redshift(spark, 
#                           env, 
#                           aws_access_key, 
#                           aws_secret_key,
#                           dataset="nypd_complaint",
#                           s3_uri="s3a://nypd-complaint/raw/nypd-complaint.csv",
#                           sql="""
#                           SELECT CMPLNT_FR_DT, 
#                                  COUNT(1) AS CNT
#                           FROM nypd_complaint
#                           GROUP BY CMPLNT_FR_DT
#                           ORDER BY CNT DESC;
#                           """ 
#                           )
#
#   # Transform & create NYC weather CSV file for redshift
#   create_csv_for_redshift(spark, 
#                           env, 
#                           aws_access_key, 
#                           aws_secret_key,
#                           dataset="nyc_weather",
#                           s3_uri="s3a://nypd-complaint/raw/nyc-weather.csv",
#                           sql="""
#                           SELECT to_date(DATE, 'YYYY-MM-DD') AS date, 
#                                  MAX(HourlyDryBulbTemperature) AS HighTemp,
#                                  MIN(HourlyDryBulbTemperature) AS LowTemp 
#                           FROM nyc_weather 
#                           GROUP BY to_date(DATE, 'YYYY-MM-DD') 
#                           ORDER BY date DESC;
#                           """ 
#                           )
#
#   # Create time dimension data
#   create_csv_for_redshift(spark, 
#                           env, 
#                           aws_access_key, 
#                           aws_secret_key,
#                           dataset="date_dimension",
#                           s3_uri="s3a://nypd-complaint/nypd-complaint.csv",
#                           sql="""
#                           SELECT CMPLNT_NUM,
#                                  CMPLNT_FR_DT,
#                                  to_timestamp(string(CMPLNT_FR_DT) + string(CMPLNT_FR_TM)) AS CMPLNT_TIMESTAMP
#                           FROM date_dimension
#                           ORDER BY CMPLNT_TIMESTAMP;
#                           """ 
#                           )
#

if __name__ == "__main__":
    main()
